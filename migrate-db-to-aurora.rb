#!/usr/bin/env ruby

require 'aws-sdk'
require 'mysql2'
require 'optparse'

def setup_read_replica(client, options)
  resp = client.describe_db_instances({db_instance_identifier: options[:db]})

  if resp.db_instances.size > 1
    raise 'Multiple responses. Aborting'
  end

  if resp.db_instances[0].db_instance_status != "available"
    raise "Source DB not available"
  end

  #Check for an existing database
  begin
    resp = client.describe_db_instances({db_instance_identifier: options[:read_rep]})

    if resp.db_instances.size > 0
      if resp.db_instances[0].read_replica_source_db_instance_identifier == options[:db]
        puts "Read Replica Currently exists. Skipping"
        return Aws::RDS::DBInstance.new(resp.db_instances[0].db_instance_identifier, {client: client})
      else
        raise "Target read replica currently exists but is not configured as a read replica of source db"
      end
    end
  rescue Aws::RDS::Errors::DBInstanceNotFound
  end

  puts "Creating Read Replica %s" % [options[:read_rep]]
  puts options[:read_rep]

  resp = client.create_db_instance_read_replica({
    db_instance_identifier: options[:read_rep],
    source_db_instance_identifier: options[:db],
    })

  replica = Aws::RDS::DBInstance.new(resp.db_instance.db_instance_identifier, {client: client})
  while replica.db_instance_status != "available"
    puts "Waiting for replica to become available"
    sleep 60
  end

  puts "Replica Created. Current Status %s " % replica.db_instance_status

  replica
end

def configure_read_rep(replica)

  if replica.backup_retention_period > 0
    return
  end
  puts "Configuring read replica"

  replica.modify({
    backup_retention_period: 1,
    apply_immediately: true
    })

    
  while replica.pending_modified_values.values.any? { |value| !value.nil? }
    puts "Waiting for modifications to be applied"
    sleep 30
  end

  while replica.db_instance_status != "available"
    puts "Waiting for replica to become available after modify"
    sleep 30
  end
end

def stop_replication(mysql)
  resp = mysql.query("CALL mysql.rds_stop_replication")
  resp.free
end

def create_snapshot(client,replica)
  puts "Creating Snapshot: %s" % [replica.db_instance_identifier + "-aurora"]
  resp = client.create_db_snapshot({
    db_snapshot_identifier: replica.db_instance_identifier + "-aurora",
    db_instance_identifier: replica.db_instance_identifier
    })

  while resp.db_snapshot.status != "available"
    puts "Waiting for Snapshot to become ready"
    sleep 60
  end

  resp.db_snapshot.db_snapshot_identifier
end

def create_aurora(client,options, replica, snapshot)
  start = Time.now

  puts "Starting Aurora creation at: #{start}"
  resp = client.restore_db_cluster_from_snapshot({
    db_cluster_identifier: options[:target],
    snapshot_identifier: snapshot,
    engine: "aurora",
    db_subnet_group_name: replica.db_subnet_group.db_subnet_group_name,
    vpc_security_group_ids: replica.vpc_security_groups.collect { |x| x.vpc_security_group_id }
  })
  
  aurora = Aws::RDS::DBInstance.new(resp.db_cluster.db_cluster_identifier, {client: client})
  while aurora.db_instance_status != "available"
    puts "Waiting for aurora cluster to become available"
    sleep 60
  end
  endTime=Time.now

  puts "Aurora Cluster Created. Current Status %s " % replica.db_instance_status
  puts "Creation Finished at: #{endTime}. Total of #{endTime-start} seconds"
  aurora
end

begin
  options = {}
  OptionParser.new do |opts|
    opts.banner = "Usage: migrate-db-to-aurora.rb [options]"
    opts.on('-d','--database NAME', 'Database to convert') do |d|
      options[:db] = d
    end
    opts.on('-u', '--user USERNAME', 'User to perform operations with') do |u|
      options[:user] = u
    end
    opts.on('-p', '--pass PASSWORD', 'Password to perform operations with') do |p|
      options[:pass] = p
    end
    opts.on('-t', '--target TARGET', 'Target Database ID') do |t|
      options[:target] = t
    end
    opts.on('-s', '--stage STAGE', 'Stage to resume migration from [1-4]') do |s|
      options[:stage] = s.to_i
    end
  end.parse!

  raise OptionParser::MissingArgument if options[:db].nil?
  raise OptionParser::MissingArgument if options[:user].nil?
  raise OptionParser::MissingArgument if options[:pass].nil?

  if options[:target].nil?
    options[:target] = options[:db] + "-migrated"
  end

  if options[:stage].nil?
    options[:stage] = 1
  end

  puts "
  Migrating Database: %s to %s
  Using User: %s
  Starting at Stage %i
  " % [options[:db], options[:target], options[:user], options[:stage]]

  options[:read_rep] = options[:db] + "-readRep"

  client=Aws::RDS::Client.new(region: "us-east-1")

  puts options
  #Stage 1 Replication Setup
  #Create read replica of target DB
  replica = setup_read_replica(client, options)
  #Modify replica to have a backup-retention-period of 1 day. Apply immediatly
  configure_read_rep(replica)


  #Stage 2 Create Aurora
  replicaMysql = Mysql2::Client.new(:host => replica.endpoint.address,:username => options[:user], :password => options[:pass])
  #Stop Replication of client
  stop_replication(replicaMysql)

  #Create snapshot of replica
  snapshot = create_snapshot(client, replica)

  #Create restore-db-cluster-from-snapshot with auroa
  aurora = create_aurora(client, options, replica, snapshot)
  #Stage 3 Begin Replication
  #Get Master Status for Replica

  # Set external master for Auroa

  # Start Replication

  #Wait for lag to be 0 in Aurora

  # Start Replication on Rep
rescue Exception => msg
  puts "Error!"
  puts msg
  puts msg.backtrace.join("\n")
end
