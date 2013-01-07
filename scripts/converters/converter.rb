require 'rdf'
require 'rdf/turtle'
require 'rdf/virtuoso'

module Nanopublication
	class RDF_Converter

		def initialize(options, header_prefix='#')
			@input = options[:input]
			@output = options[:output]
			@header_prefix = header_prefix

			# useful stuff for serializing graph.
			@prefixes = {}
			@base = RDF::Vocabulary.new(options[:base_url])

			# tracking converter progress
			@line_number = 0  # incremented after a line is read from input
			@row_index = 0 # incremented before a line is converted.

      @num_rows = 184827 # 14 # 184827 # no ETA calculation if set to nil
      @previous_time = nil
      @total_time = 0
      @total_triples = 0
      @tripleCache = Array.new()

    end

    def convert()
      File.open(@input, 'r') do |f|
        while line = f.gets
          @line_number += 1
          if line[0] == @header_prefix
            convert_header_row(line.strip)
          else
            convert_row(line.strip)
          end
        end
      end
    end

    def print_load_stats(num_statements)
      if @previous_time.nil?
        puts "starting load statistics"
        @previous_time = Time.now()
        @total_triples = num_statements
        return
      end
      now = Time.now()
      @total_time += now - @previous_time
      @previous_time = now

      @total_triples += num_statements
      triplesPerSec = @total_triples / (@total_time+1) # +1 to avoid div by 0

      triplesEst = (@total_triples/(@row_index+1)) * (@num_rows - @row_index)
      eta = now + triplesEst/(triplesPerSec+1)

      if @num_rows.nil?
        puts "#{triplesPerSec} triples/sec"
      else
        puts "#{triplesPerSec} triples/sec, #{triplesEst} est total, #{eta} ETA"
      end
    end

    def convert_header_row(row)
			# do something
			puts 'header'
		end

		def convert_row(row)
			# do something
			@row_index += 1
			puts 'row'
		end

    protected
    def insertGraph(g, triples) # make g an optional argument?
      @tripleCache.push(*triples)
      if @tripleCache.length < 100
        return
      else
        for s, p, o in @tripleCache do
          if g.nil?
            @repository.insert([s.to_uri, p, o])
          else
            @repository.insert([s.to_uri, p, o, g.to_uri])
          end
        end
        print_load_stats(@tripleCache.length())
        @tripleCache = Array.new()
      end
    end

    def insertGraphV(g, triples)
      @tripleCache.push(triples)
      if @tripleCache.length < 1000
        return
      else
        query = RDF::Virtuoso::Query
        query = query.insert_data(*triples).graph(RDF::URI.new("http://test.com"))
        @repository.insert(query)
        print_load_stats(@tripleCache.length())
        @tripleCache = Array.new()
      end
    end

    def virt_test()
      uri        = "http://localhost:8890/sparql"
      update_uri = "http://localhost:8890/sparql-auth"
      repo       = RDF::Virtuoso::Repository.new(uri)#,
                                                 #:update_uri => update_uri,
                                                 #:username => 'admin',
                                                 #:password => 'secret',
                                                 #:auth_method => 'digest')
      query = RDF::Virtuoso::Query
      query2 = RDF::Virtuoso::Query
      graph = RDF::URI.new("http://test.com")
      subject = RDF::URI.new("http://subject")
      predicate = RDF::URI.new("http://predicate")

      query = query.insert_data([subject, predicate, "object"],[subject, predicate, "bladi"]).graph(graph)#.where([subject, :p, :o])
      result = repo.insert(query)
      puts result.to_yaml
      puts "finished insert to virtuoso"

      query2  = query2.select.graph(graph).where([subject, :p, :o])
      #puts query2.to_yaml
      result = repo.select(query2)
      #puts result.to_yaml
      puts "number of results: #{result.first[:count].to_i}"
    end

    def save_to_file()
			RDF::Turtle::Writer.open(@output) do |writer|
				writer.prefixes = @prefixes
				writer.base_uri = @base
				writer << graph
			end
		end

	end
end
