module Kafkat
  class Config
    CONFIG_PATHS = [
      '~/.kafkatcfg',
      '/etc/kafkatcfg'
    ]

    class NotFoundError < StandardError; end
    class ParseError < StandardError; end

    attr_reader :kafka_path
    attr_reader :log_path
    attr_reader :zk_path
    attr_reader :json_files_path

    def self.load!
      string = nil
      e = nil

      CONFIG_PATHS.each do |rel_path|
        begin
          path = File.expand_path(rel_path)
          string = File.read(path)
          break
        rescue => e
        end
      end

      raise e if e && string.nil?

      json = JSON.parse(string)
      self.new(json)

    rescue Errno::ENOENT
      raise NotFoundError
    rescue JSON::JSONError
      raise ParseError
    end

    def initialize(json)
      @kafka_path = json['kafka_path']
      @log_path = json['log_path']
      @zk_path = json['zk_path']
      @json_files_path = json['json_files_path']
      if !@json_files_path || !File.exist?(@json_files_path)
        raise ArgumentError, "The directory \"json_files_path\": \"#{@json_files_path}\" does not exit."
      end
    end
  end
end
