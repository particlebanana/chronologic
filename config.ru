#require "chronologic"
require File.join(File.dirname(__FILE__), 'lib/chronologic')

keyspace = ENV.fetch('KEYSPACE', "ChronologicTest")
puts "Using #{keyspace}"
Chronologic.driver = :MongoDB
Chronologic.connection = Mongo::Connection.new("localhost", 27017).db(keyspace)

logger = Logger.new(STDOUT)
logger.level = Logger::DEBUG

Chronologic::Service::App.logger = logger
Chronologic::Service::Schema::MongoDB.logger = logger
run Chronologic::Service::App.new