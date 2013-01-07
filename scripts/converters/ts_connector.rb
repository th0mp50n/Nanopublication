module TS_connector

  class TS_connector
    def initialize(options)
      puts "in TS_connector constructor"
    end

    def setupConnection()
      puts "setting up connection"
    end

    def insertGraph(g, triples)
      puts "inserting into graph"
    end

    def countStatements()
      puts "counting statements"
    end

    def clearRepository()
      puts "clearing repository"
    end

    def closeConnection()
      puts "closing connection"
    end
  end

  class AllegroConnector < TS_connector
    def initialize(options)
      default = {
          :server => 'localhost',
          :port => 8890,
          :base_url => 'http://rdf.biosemantics.org/nanopubs/riken/fantom5/'
      }
      options = default.merge(options)
      super(options)

      @server = AllegroGraph::Server.new(:host=>options[:host], :port=>options[:port],
                                         :username=>"agraph", :password=>"agraph")
      @catalog = options[:catalog].nil? ? @server : AllegroGraph::Catalog.new(@server, options[:catalog])
      @repository = RDF::AllegroGraph::Repository.new(:server=>@catalog, :id=>options[:repository])
      @repository.clear
    end

    def insertGraph(g, triples) # make g an optional argument?
      for s, p, o in triples do
        if g.nil?
          @repository.insert([s.to_uri, p, o])
        else
          @repository.insert([s.to_uri, p, o, g.to_uri])
        end
        #print_load_stats(@tripleCache.length())
      end
    end
  end

  class VirtuosoConnector < TS_connector
    def initialize(options)
      default = {
          :server => 'localhost',
          :port => 10035,
          :base_url => 'http://rdf.biosemantics.org/nanopubs/riken/fantom5/'
      }
      options = default.merge(options)
      super(options)

      uri        = "http://localhost:8890/sparql"
      @repository = RDF::Virtuoso::Repository.new(uri)
    end

    def insertGraph(g, triples) # how to deal with no/default graph in ruby-virtuoso?
      query = RDF::Virtuoso::Query
      query = query.insert_data(*triples).graph(RDF::URI.new("http://test.com"))
      @repository.insert(query)
      #print_load_stats(@tripleCache.length())
    end
  end

end