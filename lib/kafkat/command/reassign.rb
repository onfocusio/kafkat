class Hash
  def hmap!(&block)
    self.keys.each do |key|
      hash = block.call(key, self[key])

      self[hash.keys.first] = hash[hash.keys.first]
      self.delete(key)
    end
    self
  end

  def hmap(&block)
    h = Hash.new
    self.keys.each do |key|
      hash = block.call(key, self[key])

      h[hash.keys.first] = hash[hash.keys.first]
    end
    h
  end
end


module Kafkat
  module Command
    class Reassign < Base
      register_as 'reassign'

      usage 'reassign [topics] [--brokers <ids>] [--replicas <n>]',
            'Begin reassignment of partitions.'

      def run
        topic_names = ARGV.shift unless ARGV[0] && ARGV[0].start_with?('--')

        all_brokers = zookeeper.get_brokers

        topics = nil
        if topic_names
           topics_list = topic_names.split(',')
           topics = zookeeper.get_topics(topics_list)
        end
        topics ||= zookeeper.get_topics

        opts = Trollop.options do
          opt :brokers, "replica set (broker IDs)", type: :string
          opt :replicas, "number of replicas (count)", type: :integer
        end

        broker_ids = opts[:brokers] && opts[:brokers].split(',').map(&:to_i)
        replica_count = opts[:replicas]

        broker_ids ||= zookeeper.get_brokers.values.map(&:id)

        all_brokers_id = all_brokers.values.map(&:id)
        broker_ids.each do |id|
          if !all_brokers_id.include?(id)
            print "ERROR: Broker #{id} is not currently active.\n"
            exit 1
          end
        end

        # *** This logic is duplicated from Kakfa 0.8.1.1 ***

        assignments = []
        broker_count = broker_ids.size

        # Helper structures
        @replica_count = replica_count
        @nodes_lists_replicas = {}
        @nodes_lists_isr = {}
        @partitions_lists={}
        @partitions_lists_old={}

        topics.each do |_, t|
          # This is how Kafka's AdminUtils determines these values.
          partition_count = t.partitions.size
          topic_replica_count = replica_count || t.partitions[0].replicas.size

          if topic_replica_count > broker_count
            print "ERROR: Replication factor (#{topic_replica_count}) is larger than brokers (#{broker_count}).\n"
            exit 1
          end

          # start_index = Random.rand(broker_count)
          # replica_shift = Random.rand(broker_count)

          # t.partitions.each do |p|
          #   replica_shift += 1 if p.id > 0 && p.id % broker_count == 0
          #   first_replica_index = (p.id + start_index) % broker_count

          #   replicas = [broker_ids[first_replica_index]]

          #   (0...topic_replica_count-1).each do |i|
          #     shift = 1 + (replica_shift + i) % (broker_count - 1)
          #     index = (first_replica_index + shift) % broker_count
          #     replicas << broker_ids[index]
          #   end

          #   replicas.reverse!
          #   assignments << Assignment.new(t.name, p.id, replicas)
          # end

          t.partitions.each do |p|
            @partitions_lists[p.topic_name + ' ' + p.id.to_s] ||= {'name'=>p.topic_name, 'id'=>p.id, 'replicas'=>{},'isr'=>{}}
            @partitions_lists_old[p.topic_name + ' ' + p.id.to_s] ||= {'name'=>p.topic_name, 'id'=>p.id, 'replicas'=>{},'isr'=>{}}

            p.replicas.each do |node|
              @partitions_lists[p.topic_name + ' ' + p.id.to_s]['replicas'][node] = ''
              @partitions_lists_old[p.topic_name + ' ' + p.id.to_s]['replicas'][node] = ''
              @nodes_lists_replicas[node] ||= {}
              @nodes_lists_replicas[node][p.topic_name + ' ' + p.id.to_s] = ''
            end

            p.isr.each do |node|
              @partitions_lists[p.topic_name + ' ' + p.id.to_s]['isr'][node] = ''
              @partitions_lists_old[p.topic_name + ' ' + p.id.to_s]['isr'][node] = ''

              # nodes_sizes_isr[node] ||= 0
              # nodes_sizes_isr[node] += 1
              @nodes_lists_isr[node] ||= {}
              @nodes_lists_isr[node][p.topic_name + ' ' + p.id.to_s] = ''
            end
          end
        end


        # Add a topic partition to a specific node
        def add_partition_to_node( tp, node )

          @nodes_lists_replicas[node][tp] = ''
          @partitions_lists[tp]['replicas'] ||= {}
          @partitions_lists[tp]['replicas'][node] = ''
        end


        # Add a topic partition to lowest unbalanced nodes
        # you can specify the number of times the partition must be added to fit its minimal replica count
        def add_partition_to_all_lowest( tp, nb_partitions_to_add )

          nodes_sizes_replicas = @nodes_lists_replicas.hmap { |k,v| { k => v.size } }
          nodes_lowest = nodes_sizes_replicas.sort_by{|k,v| v}.map { |a| a[0] }

          nodes_lowest.each do |node|
            break if nb_partitions_to_add <= 0

            unless @nodes_lists_replicas[node].has_key?(tp)
              add_partition_to_node( tp, node )
              nb_partitions_to_add -= 1
            end
          end
        end


        def remove_partition_from_node( tp, node )

            @nodes_lists_replicas[node].delete tp
            @nodes_lists_isr[node].delete tp
            @partitions_lists[tp]['replicas'].delete(node)
            @partitions_lists[tp]['isr'].delete(node)
        end


        # Remove excess replicas from node
        # If nb_partitions_to_remove is nil, then remove all excess replicas
        # First remove partitions which are not in ISR and already replicated enough times on other nodes of the cluster
        # Then regardless of ISR
        def remove_excess_replicas_from_node( node, nb_partitions_to_remove )

          # First remove partitions which are not in ISR and already replicated enough times on other nodes of the cluster

          replicas_not_isr = @nodes_lists_replicas[node].keys - @nodes_lists_isr[node].keys

          replicas_not_isr_over_replicated = replicas_not_isr.keep_if { |tp| @partitions_lists[tp]['replicas'].keys.size > @replica_count }

          replicas_not_isr_over_replicated.each do |tp|
            break if (!nb_partitions_to_remove.nil? && nb_partitions_to_remove <= 0)

            remove_partition_from_node( tp, node )

            nb_partitions_to_remove -= 1 unless nb_partitions_to_remove.nil?
          end


          # Then remove partitions which regardless of ISR and already replicated enough times on other nodes of the cluster

          replicas_over_replicated = @nodes_lists_replicas[node].keys.keep_if { |tp| @partitions_lists[tp]['replicas'].keys.size > @replica_count }

          replicas_over_replicated.each do |tp|
            break if (!nb_partitions_to_remove.nil? && nb_partitions_to_remove <= 0)

            remove_partition_from_node( tp, node )

            nb_partitions_to_remove -= 1 unless nb_partitions_to_remove.nil?
          end

        end


        # Migrate excess replicas from node to lowest unbalanced nodes
        # It chooses absent replicas from lowest unbalanced nodes, to make sure we fill the lowest
        # BUG: There is very probably an issue here in case the lowest node has less than `nb_partitions_to_remove` absent partitions from source node
        def migrate_excess_replicas_from_node( node, nb_partitions_to_remove )

          # This method is not efficient, but that is not something you run often
          nb_partitions_to_remove.times do |i|

            # Find out the lowest balanced nodes
            nodes_sizes_replicas = @nodes_lists_replicas.hmap { |k,v| { k => v.size } }
            node_lowest = nodes_sizes_replicas.sort_by{|k,v| v}[0][0]

            # Find out which partitions the lowest balanced node does not have and which can be migrated from this node
            partition_to_migrate = (@nodes_lists_replicas[node].keys - @nodes_lists_replicas[node_lowest].keys)[0]

            remove_partition_from_node( partition_to_migrate, node )
            add_partition_to_node( partition_to_migrate, node_lowest )

          end

          # # Find out the lowest balanced nodes
          # nodes_sizes_replicas = @nodes_lists_replicas.hmap { |k,v| { k => v.size } }
          # nodes_lowest = nodes_sizes_replicas.sort_by{|k,v| v}.map { |a| a[0] }

          # nodes_lowest.each do |node_lowest|
          #   break if nb_partitions_to_remove <= 0

          #   # Find out which partitions the lowest balanced node does not have and which can be migrated from this node
          #   partitions_to_migrate = @nodes_lists_replicas[node].keys - @nodes_lists_replicas[node_lowest].keys

          #   partitions_to_migrate.each do |tp|
          #     break if nb_partitions_to_remove <= 0

          #     remove_partition_from_node( tp, node )
          #     add_partition_to_node( tp, node_lowest )

          #     nb_partitions_to_remove -= 1
          #   end
          # end
        end


        ###
        #   At this point, the helper structures represent the current repartition of partitions
        #   We will modify the helper structures to balance the partitions accross brokers whith the minimum number of partitions moves
        ###

        # Add under replicated partitions to lowest unbalanced nodes
        partitions_under_replicated = @partitions_lists.keys.keep_if { |tp| @partitions_lists[tp]['replicas'].size < @replica_count }
        partitions_under_replicated.each do |tp|
          how_many = @replica_count - @partitions_lists[tp]['replicas'].size
          add_x_to_all_lowest( tp, how_many )
        end


        # Begin leaning and draining nodes with the most partitions towards the lowest unbalanced nodes
        2.times do |i|

          # List all nodes from biggest to lowest balanced
          nodes_sizes_replicas = @nodes_lists_replicas.hmap { |k,v| { k => v.size } }
          nodes_biggest = nodes_sizes_replicas.sort_by{|k,v| v}.reverse.map { |a| a[0] }

          # helper count of partition replicas to balance
          nodes_left_to_balance = nodes_biggest.size
          nb_partitions_left_to_balance = @replica_count * @partitions_lists.keys.size

          # Remove excess replicas on all nodes from biggest to lowest unbalanced nodes, until balance is achieved
          nodes_biggest.each do |node|

            # Figure out the numbers of partitions each node should have to be balanced
            # I am wary about float operations, for example to do a 5.00000001.ceil()
            # nb_partitions_should_have = ( nb_partitions_left_to_balance / nodes_left_to_balance.to_f ).ceil
            nb_partitions_should_have = nb_partitions_left_to_balance / nodes_left_to_balance
            nb_partitions_should_have += 1 if (nb_partitions_left_to_balance % nodes_left_to_balance) > 0


            # nb_partitions_to_remove = @nodes_lists_replicas[node].keys.size - nb_partitions_should_have.ceil
            nb_partitions_to_remove = @nodes_lists_replicas[node].keys.size - nb_partitions_should_have
            remove_excess_replicas_from_node( node, nb_partitions_to_remove) if nb_partitions_to_remove > 0

            # nb_partitions_to_remove = @nodes_lists_replicas[node].keys.size - nb_partitions_should_have.ceil
            nb_partitions_to_remove = @nodes_lists_replicas[node].keys.size - nb_partitions_should_have
            migrate_excess_replicas_from_node( node, nb_partitions_to_remove) if nb_partitions_to_remove > 0


            nodes_left_to_balance -= 1
            nb_partitions_left_to_balance = nb_partitions_left_to_balance - nb_partitions_should_have

          end
        end

        # Remove excess replicas on all nodes from lowest unbalanced nodes to biggest nodes. This is to enforce replica count, and if the whole algorithm is fine, it should do nothing
        nodes_sizes_replicas = @nodes_lists_replicas.hmap { |k,v| { k => v.size } }
        nodes_biggest = nodes_sizes_replicas.sort_by{|k,v| v}.reverse.map { |a| a[0] }
        nodes_biggest.each do |node|
          remove_excess_replicas_from_node( node, nil )
        end


        require 'pp'
        puts "\nCurrent partition assignment:"
        pp @partitions_lists_old.hmap { |k,v| { k => { 'replicas'=>v['replicas'], 'isr'=>v['isr']} } }
        puts "\nOptimized partition assignment with minimal partition migrations:"
        pp @partitions_lists.hmap { |k,v| { k => { 'replicas'=>v['replicas'], 'isr'=>v['isr']} } }
        puts ''


        @partitions_lists.each_key do |tp|
          assignments << Assignment.new( @partitions_lists[tp]['name'],
                                         @partitions_lists[tp]['id'],
                                         @partitions_lists[tp]['replicas'].keys,
                                       )
        end


        # ****************

        prompt_and_execute_assignments(assignments)
      end
    end
  end
end
