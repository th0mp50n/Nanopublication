require 'rdf'
require 'rdf/turtle'

module Nanopublication
	class RDF_Converter
    NumRows = 184827 # 14 # 184827

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

      @start_time = Time.now
		end

		def convert()
			File.open(@input, 'r') do |f|
				while line = f.gets
					@line_number += 1
					if line[0] == @header_prefix
						convert_header_row(line.strip)
          else
					  convert_row(line.strip)
            print_load_stats()
					end
				end
			end
		end

    def print_load_stats()
      triplesLoaded = @repository.count()
      triplesEst = (triplesLoaded / (@row_index+1)) * (NumRows - @row_index)
      timePassed = Time.now.to_i - @start_time.to_i
      triplesPerSec = triplesLoaded / (timePassed+1)
      estEndTime = Time.now + triplesEst/(triplesPerSec+1)
      puts "In store: #{triplesLoaded}, estimated total: #{triplesLoaded+triplesEst} ETA: #{estEndTime} (#{triplesPerSec} Tps)"
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
      for s, p, o in triples do
        if g.nil?
          @repository.insert([s.to_uri, p, o])
        else
          @repository.insert([s.to_uri, p, o, g.to_uri])
        end
      end
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
