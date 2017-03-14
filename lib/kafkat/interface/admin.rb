require 'time'

module Kafkat
  module Interface
    class Admin
      class ExecutionFailedError < StandardError; end

      attr_reader :kafka_path
      attr_reader :zk_path
      attr_reader :json_files_path

      def initialize(config)
        @kafka_path = config.kafka_path
        @zk_path = config.zk_path
        @json_files_path = config.json_files_path
      end

      def elect_leaders!(partitions)
        file = File.new File.join(@json_files_path, "elect-leaders_#{Time.now.xmlschema}.json"), "w"

        json_partitions = []
        partitions.each do |p|
          json_partitions << {
            'topic' => p.topic_name,
            'partition' => p.id
          }
        end

        json = {'partitions' => json_partitions}
        file.write(JSON.dump(json))
        file.close

        puts "Using JSON file: " + file.path

        run_tool(
          'kafka-preferred-replica-election',
            '--path-to-json-file', file.path
        )
      end

      def reassign!(assignments)
        file_name = "reassign_#{Time.now.xmlschema}.json"
        file = File.new File.join(@json_files_path, file_name), "w"

        json_partitions = []
        assignments.each do |a|
          json_partitions << {
            'topic' => a.topic_name,
            'partition' => a.partition_id,
            'replicas' => a.replicas
          }
        end

        json = {
          'partitions' => json_partitions,
          'version' => 1
        }

        file.write(JSON.dump(json))
        file.close

        puts "Using JSON file: " + file.path
        puts "Run this command to check the status: kafkat verify-reassign #{file_name}"

        run_tool(
          'kafka-reassign-partitions',
            '--execute',
            '--reassignment-json-file', file.path
        )
      end

      def verify_reassign(file_name)
        file =
          if File.exist? file_name
            File.new file_name
          else
            File.new File.join(@json_files_path, file_name)
          end

        puts "Using JSON file: " + file.path

        run_tool(
          'kafka-reassign-partitions',
            '--verify',
            '--reassignment-json-file', file.path
        )
      end

      def shutdown!(broker_id, options={})
        args = ['--broker', broker_id]
        args += ['--num.retries', options[:retries]] if options[:retries]
        args += ['--retry.interval.ms', option[:interval]] if options[:interval]

        run_tool(
          'kafka-run-class',
            'kafka.admin.ShutdownBroker',
            *args
        )
      end

      def run_tool(name, *args)
        path = File.join(kafka_path, "bin/#{name}.sh")
        # The scripts in the Confluent package does not have .sh extensions
        if !File.exist? path
          path = File.join(kafka_path, "bin/#{name}")
        end
        args += ['--zookeeper', "\"#{zk_path}\""]
        args_string = args.join(' ')
        result = `#{path} #{args_string}`
        raise ExecutionFailedError if $?.to_i > 0
        result
      end
    end
  end
end
