#!/usr/bin/ruby -w 

require 'rubygems'
require 'mongo'

connection = Mongo::Connection.new
db = connection.db("flowmongo")
# need to make this collection capped
coll = db.collection("httpmemcache", :capped =>true, :size=>10000000)
#coll = db.collection("httpmemcache")

#coll.create_index([["flowSampleType","memcache_op_value_bytes","memcache_op_duration_uS","http_bytes","http_duration_uS"]])
coll.create_index("flowSampleType")

DATAGRAM_METRICS = Set[:unixSecondsUTC, :agent]
SAMPLE_METRICS = Set[:socket4_remote_ip, :socket4_local_port, :socket4_remote_port, :memcache_op_protocol, :memcache_op_cmd, :memcache_op_nkeys, :memcache_op_value_bytes, :memcache_op_duration_uS, :memcache_op_status, :memcache_op_key, :flowSampleType, :http_method, :http_protocol, :http_uri, :http_host, :http_referrer, :http_useragent, :http_bytes, :http_duration_uS, :http_status ]


@valuehash = Hash.new
@metahash = Hash.new

ARGF.each do |e|
   # probably want to insert numerics values as numbers (http_status,duration,etc)
   # do we need an index on UTC if capped, prolly yes, store as Date format
    metric, value  = e.split

   # otherwise, everything goes in as a string
   storevalue = case metric
     when "socket4_local_port","socket4_remote_port","memcache_op_protocol", "memcache_op_cmd", "memcache_op_nkeys", "memcache_op_status", "http_method", "http_protocol", "http_bytes", "http_status", "memcache_op_duration_uS", "http_duration_uS" then value.to_i
     when "memcache_op_value_bytes" then value.to_f
     when "unixSecondsUTC" then Time.at(value.to_i)
     else value
    end

    @metahash.store(metric,storevalue) if DATAGRAM_METRICS.include?(metric.to_sym) 
    @valuehash.store(metric,storevalue) if SAMPLE_METRICS.include?(metric.to_sym) 

    if ( metric == 'endSample') 
        next unless ['memcache','http'].include?@valuehash['flowSampleType']
   #   puts "inserting type #{@valuehash['flowSampleType']}"
      begin
        # combine meta and value
        inserthash = @metahash.merge(@valuehash)
        id = coll.insert(inserthash)
      rescue => e
         puts @valuehash.inspect
    #     raise e 
         puts e 
      end
       @valuehash = Hash.new
    end

    if ( metric == 'endDatagram') 
#        puts @valuehash.keys
         @metahash = Hash.new
     end
end
