module Chronologic::Service::Schema::MongoDB
  mattr_accessor :logger

  MAX_SUBSCRIPTIONS = 50_000
  MAX_TIMELINES = 50_000

  def self.create_object(key, attrs)
    log "create_object(#{key})"

    connection[:Object].insert({ "_id" => key, "value" => attrs })
  end

  def self.remove_object(object_key)
    log("remove_object(#{object_key})")

    connection[:Object].remove({ "_id" => object_key })
  end

  def self.object_for(object_key)
    log("object_for(#{object_key})")

    case object_key
    when String
      obj = connection[:Object].find({ "_id" => object_key.to_s }).map { |e| e['value'] }.to_a

      obj.empty? ? Hash.new : obj.first
    when Array
      return {} if object_key.empty?
      values = {}
      connection[:Object].find({ "_id" => { "$in" => object_key } }).to_a.map{ |e| values[e['_id']] = e['value'] }

      values
    end
  end

  def self.create_subscription(timeline_key, subscriber_key, backlink_key='')
    log("create_subscription(#{timeline_key}, #{subscriber_key}, #{backlink_key})")

    # Insert a new record if one does not exists
    connection[:Subscription].insert({ "_id" => subscriber_key, "subscribers" => []}, { "_id" => { "$exists" => false } })

    # Add subscription to set
    connection[:Subscription].update({ "_id" => subscriber_key}, { "$addToSet" => {"subscribers" => {timeline_key => backlink_key} } })
  end

  def self.remove_subscription(timeline_key, subscriber_key)
    log("remove_subscription(#{timeline_key}, #{subscriber_key}")

    connection[:Subscription].update({ "_id" => subscriber_key }, { "$pull" => {"subscribers" => { timeline_key => {"$exists" => true} } } })
  end

  def self.subscribers_for(timeline_key)
    log("subscribers_for(#{timeline_key})")

    subscribers = []
    case timeline_key
    when String
      connection[:Subscription].find({ "_id" => timeline_key }).limit(MAX_SUBSCRIPTIONS).to_a.each do |subscription|
        subscription['subscribers'].map { |sub| subscribers << sub.keys.first}
      end
    when Array
      return [] if timeline_key.empty?
      connection[:Subscription].find({ "_id" => { "$in" => timeline_key } }).limit(MAX_SUBSCRIPTIONS).to_a.each do |subscription|
        subscription['subscribers'].each { |sub| subscribers << sub.keys.first}
      end
    end

    subscribers
  end

  def self.followers_for(timeline_key)
    followers = []
    connection[:Subscription].find({ "_id" => timeline_key }).limit(MAX_SUBSCRIPTIONS).to_a.each do |subscription|
      subscription['subscribers'].each { |sub| followers << sub.values.first }
    end

    followers
  end

  def self.create_event(event_key, data)
    log("create_event(#{event_key})")

    connection[:Event].insert({ "_id" => event_key, "value" => data })
  end

  def self.update_event(event_key, data)
    log("update_event(#{event_key})")

    connection[:Event].update({"_id" => event_key}, {"$set" => {"value" => data}})
  end

  def self.remove_event(event_key)
    log("remove_event(#{event_key})")

    connection[:Event].remove({ "_id" => event_key })
  end

  def self.event_exists?(event_key)
    log("event_exists?(#{event_key.inspect})")

    obj = connection[:Event].find({ "_id" => event_key }).to_a
    obj.empty? ? false : true
  end

  def self.event_for(event_key)
    log("event_for(#{event_key.inspect})")

    case event_key
    when String
      obj = connection[:Event].find({ "_id" => event_key }).map { |e| e['value'] }.to_a

      obj.empty? ? Hash.new() : obj.first
    when Array
      return {} if event_key.empty?
      values = {}
      connection[:Event].find({ "_id" => { "$in" => event_key } }).to_a.map{ |e| values[e['_id']] = e['value'] }

      values
    end
  end

  def self.create_timeline_event(timeline, uuid, event_key)
    log("create_timeline_event(#{timeline}, #{uuid}, #{event_key})")

    # Insert a new record if one does not exists
    connection[:Timeline].insert({ "_id" => timeline, "events" => []}, { "_id" => { "$exists" => false } })

    # Add subscription to set
    connection[:Timeline].update({ "_id" => timeline}, { "$addToSet" => {"events" => {uuid => event_key} } })
  end

  def self.timeline_for(timeline, options={})
    log("timeline_for(#{timeline}, #{options.inspect})")

    count = options[:per_page] || 20
    skip = options[:page] || 0

    data = []
    timeline_data = {}
    case timeline
    when String
      connection[:Timeline].find({ "_id" => timeline }).skip(skip).limit(count).map { |e| e['events'] }.to_a.each do |event|
        event.map { |e| data << e }
      end
    when Array
      return {} if timeline.empty?
      connection[:Timeline].find({ "_id" => { "$in" => timeline } }).sort(["value", :desc]).skip(skip).limit(count).to_a.each do |objects|
        #puts objects.inspect
        #subscription['subscribers'].each { |sub| subscribers << sub.keys.first}
      end
    end
    # Ya it's ugly
    data.reverse! # force reverse chronological order
    data[0..(count.to_i - 1)].map { |e| timeline_data[e.keys.first] = e.values.first }
    timeline_data
  end

  def self.timeline_events_for(timeline, options={})
    log("timeline_events_for(#{timeline})")

    case timeline
    when String
      timeline_for(timeline, options)
    when Array
      timeline_for(timeline).inject({}) do |hsh, (timeline_key, column)| 
        hsh.update(timeline_key => column.values)
      end
    end
  end

  def self.remove_timeline_event(timeline, uuid)
    log("remove_timeline_event(#{timeline}, #{uuid})")

    connection[:Timeline].update({ "_id" => timeline }, { "$pull" => {"events" => { uuid => {"$exists" => true} } } })
  end

  def self.timeline_count(timeline)
    # Used to use connection.count_columns here, but it doesn't seem
    # to respect the :count option. There is a fix for this in rjackson's fork,
    # need to see if its merged into fauna and included in a release. ~AKK

    # But in the meantime, nothing in Gowalla is using the page count so we're
    # going to hardcode this obviously incorrect value for the time being.
    -1
  end

  # Lookup events on the specified timeline(s) and return all the events
  # referenced by those timelines.
  #
  # timeline_keys - one or more String timeline_keys to fetch events from
  #
  # Returns a flat array of events
  def self.fetch_timelines(timeline_keys, per_page=20, page='')
    event_keys = timeline_events_for(
      timeline_keys,
      :per_page => per_page,
      :page => page
    ).values.flatten

    event_for(event_keys.uniq).
      map do |k, e|
        Chronologic::Event.load_from_columns(e).tap do |event|
          event.key = k
        end
      end
  end

  # Fetch objects referenced by events and correctly populate the event objects
  #
  # events - an array of Chronologic::Event objects to populate
  #
  # Returns a flat array of Chronologic::Event objects with their object
  # references populated.
  def self.fetch_objects(events)
    object_keys = events.map { |e| e.objects.values }.flatten.uniq
    objects = object_for(object_keys)
    events.map do |e|
      e.tap do
        e.objects.each do |type, keys|
          if keys.is_a?(Array)
            e.objects[type] = keys.map { |k| objects[k] }
          else
            e.objects[type] = objects[keys]
          end
        end
      end
    end
  end

  # Convert a flat array of Chronologic::Events into a properly hierarchical
  # timeline.
  #
  # events - an array of Chronologic::Event objects, each possibly referencing
  # other events
  #
  # Returns a flat array of Chronologic::Event objects with their subevent
  # references correctly populated.
  def self.reify_timeline(events)
    event_index = events.inject({}) { |idx, e| idx.update(e.key => e) }
    timeline_index = events.inject([]) do |timeline, e|
      if e.subevent? && event_index.has_key?(e.parent)
        # AKK: something is weird about Hashie::Dash or Event in that if you 
        # push objects onto subevents, they are added to an object that is 
        # referenced by all instances of event. So, these dup'ing hijinks are 
        subevents = event_index[e.parent].subevents.dup
        subevents << e
        event_index[e.parent].subevents = subevents
      else
        timeline << e.key
      end
      timeline
    end
    timeline_index.map { |key| event_index[key] }
  end

  def self.batch
    connection.batch { yield }
  end

  def self.connection
    Chronologic.connection
  end

  def self.log(msg)
    return unless logger
    logger.debug(msg)
  end

end

