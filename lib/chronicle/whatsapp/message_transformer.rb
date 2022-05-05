require 'chronicle/etl'

module Chronicle
  module Whatsapp
    class MessageTransformer < Chronicle::ETL::Transformer
      register_connector do |r|
        r.provider = 'whatsapp'
        r.description = 'a row from a local whatsapp database'
        r.identifier = 'message'
      end

      def transform
        set_actors
        record = build_messaged
        record
      end

      def timestamp
        # FIXME: handle UTC parsing better than this
        DateTime.parse(message['time']).to_time
      end

      def id
        message['ZSTANZAID']
      end

      private

      def message
        @message ||= @extraction.data
      end

      def set_actors
        participants = @extraction.meta[:participants] || participants_from_message

        me = build_identity_mine
        # Figure out the sender / receiver(s) of a message
        case message["ZISFROMME"]
        when 1
          @actor = me
          @consumers = participants.collect{|p| build_identity(p)}
        else
          sender_id = message['ZMEMBERJID'] || message['ZFROMJID']
          sender = participants.select{|p| p['ZMEMBERJID'] == sender_id}.first
          receivers = participants - [sender]

          @consumers = receivers.collect{|p| build_identity(p)}
          @actor = build_identity(sender)
        end

        raise(Chronicle::ETL::UntransformableRecordError, "Could not determine message actor") unless @actor
      end

      def participants_from_message
        [{
          'ZMEMBERJID' => message['container'],
          'ZPUSHNAME' => message['ZPARTNERNAME'],
        }]
      end

      def build_identity_mine
        record = ::Chronicle::ETL::Models::Entity.new
        record.represents = 'identity'
        record.provider = 'whatsapp'
        record.title = @extraction.meta[:me][:name]
        record.provider_id = @extraction.meta[:me][:member_id]
        record.dedupe_on = [[:represents, :provider, :provider_id]]

        record
      end

      def build_identity(contact)
        raise(Chronicle::ETL::UntransformableRecordError, "Could not build identity") unless contact

        name = if contact['ZMEMBERJID'] == @extraction.meta[:me][:member_id]
          @extraction.meta[:me][:name]
        else
          name = contact['ZPUSHNAME']
        end          

        record = ::Chronicle::ETL::Models::Entity.new
        record.represents = 'identity'
        record.provider = 'whatsapp'
        record.provider_id = contact['ZMEMBERJID']
        record.title = name
        record.dedupe_on = [[:represents, :provider, :provider_id]]

        record
      end

      def build_messaged
        record = ::Chronicle::ETL::Models::Activity.new
        record.end_at = timestamp
        record.verb = 'messaged'
        record.provider = 'whatsapp'
        record.provider_id = id
        record.dedupe_on << [:verb, :provider, :end_at]
        record.dedupe_on << [:verb, :provider, :provider_id]

        record.involved = build_message
        record.actor = @actor

        record
      end

      def build_message
        record = ::Chronicle::ETL::Models::Entity.new
        record.body = message['ZTEXT']
        record.represents = 'message'
        record.provider = 'whatsapp'
        record.provider_id = id

        record.consumers = @consumers
        record.containers = build_container if group_message?
        record.contains = build_attachment if has_attachment?

        record.dedupe_on = [[:represents, :provider, :provider_id]]

        record
      end

      def build_container
        record = ::Chronicle::ETL::Models::Entity.new
        record.title = message['ZPARTNERNAME']
        record.represents = 'thread'
        record.provider = 'whatsapp'
        record.provider_id = message['container']

        record.dedupe_on = [[:represents, :provider, :provider_id]]

        record
      end

      def build_attachment
        mime_type = message['ZVCARDSTRING']
        return unless mime_type

        type, subtype = mime_type.split("/")
        return unless ['image', 'audio', 'video'].include?(type)

        filename = @extraction.meta[:attachment_filename]
        return unless File.exist?(filename)

        recognized_text = ::Chronicle::ETL::Utils::TextRecognition.recognize_in_image(filename: filename) if type == 'image'

        attachment_data = ::Chronicle::ETL::Utils::BinaryAttachments.filename_to_base64(filename: filename, mimetype: mime_type)

        record = ::Chronicle::ETL::Models::Entity.new
        record.provider = 'whatsapp'
        record.provider_id = id
        record.title = message['ZTITLE']
        record.represents = type
        record.metadata[:ocr_text] = recognized_text if recognized_text
        record.dedupe_on = [[:provider, :provider_id, :represents]]

        attachment = ::Chronicle::ETL::Models::Attachment.new
        attachment.data = attachment_data
        record.attachments = [attachment]

        record
      end

      def has_attachment?
        message['ZMEDIALOCALPATH'] && message['ZMEDIALOCALPATH'] != "" 
      end

      def group_message?
        message['container'].match(/g\.us$/)
      end
    end
  end
end
