module Kafkat
  module Command
    class VerifyReassign < Base
      register_as 'verify-reassign'

      usage 'verify-reassign reassign_YYYY-MM-DDThh:mm:ssZ.json',
            'Verify reassignment of partitions.'

      def run
        file_name = ARGV.shift

        all_brokers = zookeeper.get_brokers

        puts admin.verify_reassign(file_name)
      end
    end
  end
end
