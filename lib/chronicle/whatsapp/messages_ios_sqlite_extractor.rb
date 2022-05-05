require 'sqlite3'

module Chronicle
  module Whatsapp 
    class MessagesIOSSqliteExtractor < Chronicle::ETL::Extractor
      register_connector do |r|
        r.provider = 'whatsapp'
        r.description = 'messages from a local sqlite backup of iOS whatsapp'
        r.identifier = 'messages'
      end

      setting :input, required: true
      setting :my_whatsapp_id, required: true
      setting :my_whatsapp_name, required: true

      def prepare
        @db = SQLite3::Database.new(db_file, results_as_hash: true)
        @messages = load_messages
        @chats = load_chats
      end

      def extract
        @messages.each do |message|
          meta = {}
          meta[:attachment_filename] = Pathname.new(db_file).dirname.join('result', 'Message', message['ZMEDIALOCALPATH']) if message['ZMEDIALOCALPATH']
          meta[:participants] = @chats[message['container']]
          meta[:me] = {
            name: @config.my_whatsapp_name,
            member_id: @config.my_whatsapp_id
          }
          yield Chronicle::ETL::Extraction.new(data: message, meta: meta)
        end
      end

      def results_count
        @messages.count
      end

      private

      def db_file
        [@config.input].flatten.first
      end

      def load_chats
        sql = <<~SQL
          SELECT
            *
          FROM
            ZWAGROUPMEMBER AS groupmember
            LEFT JOIN ZwaCHATSESSION AS chat ON groupmember.ZCHATSESSION = chat.z_pk
            left join ZWAPROFILEPUSHNAME as profile ON groupmember.ZMEMBERJID = profile.ZJID
            order by chat.zcontactjid
        SQL
        results = @db.execute(sql)
        chats = results.group_by{|x| x['ZCONTACTJID']}
      end

      def load_messages
        conditions = []
        conditions << "time > '#{@config.since.utc}'" if @config.since
        conditions << "time < '#{@config.until.utc}'" if @config.until

        sql = <<~SQL
          SELECT
            COALESCE(ZFROMJID, ZTOJID) AS container,
            ZWACHATSESSION.ZPARTNERNAME,
            datetime (zwamessage.ZMESSAGEDATE + 978307200,
              'unixepoch') AS time,
            ZWAGROUPMEMBER.ZMEMBERJID,
            ZWAMESSAGE.*,
            ZWAMEDIAITEM.*
          FROM
            ZWAMESSAGE
            LEFT JOIN ZWAGROUPMEMBER ON main.ZWAMESSAGE.ZGROUPMEMBER = main.ZWAGROUPMEMBER.Z_PK
            LEFT JOIN ZWAMEDIAITEM ON ZWAMESSAGE.z_pk = ZWAMEDIAITEM.ZMESSAGE
            LEFT JOIN ZWACHATSESSION ON ZWACHATSESSION.ZCONTACTJID = container
        SQL
        sql += " WHERE #{conditions.join(" AND ")}" if conditions.any?
        sql += " ORDER BY time DESC"
        sql += " LIMIT #{@config.limit}" if @config.limit

        results = @db.execute(sql)
      end
    end
  end
end
