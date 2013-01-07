require 'agraph'
require 'rdf'
require 'rdf/turtle'
require 'rdf/trig'
require 'optparse'
require 'rdf-agraph'
require_relative 'converter'
require_relative 'ts_connector'

module Nanopublication # putting everything inside module prevents name-space clashes?

  # Define some useful RDF vocabularies.
  FOAF = RDF::FOAF
  DC = RDF::DC
  RDFS = RDF::RDFS
  XSD = RDF::XSD
  RSO = RDF::Vocabulary.new('http://rdf.biosemantics.org/ontologies/referencesequence#')
  HG = RDF::Vocabulary.new('http://rdf.biosemantics.org/data/genomes/humangenome#')
  NCBI = RDF::Vocabulary.new('http://rdf.biosemantics.org/ontologies/ncbiassembly#')
  SO = RDF::Vocabulary.new('http://purl.org/obo/owl/SO#')
  FANTOM5 = RDF::Vocabulary.new('http://rdf.biosemantics.org/data/riken/fantom5/data#')
  PROV = RDF::Vocabulary.new('http://www.w3.org/ns/prov#')
  OBO = RDF::Vocabulary.new('http://purl.org/obo/owl/obo#')
  PAV = RDF::Vocabulary.new('http://swan.mindinformatics.org/ontologies/1.2/pav/')
  NP = RDF::Vocabulary.new('http://www.nanopub.org/nschema#')

  CAGERES = RDF::Vocabulary.new('http://www.riken.jp/data/rdf/fantom5/cage/cageresults#')
  HG19 = RDF::Vocabulary.new("http://www.riken.jp/data/rdf/fantom5/cage/hg19/")
  VOID = RDF::Vocabulary.new("http://rdfs.org/ns/void#")

  class Fantom5_RDF_Converter < RDF_Converter

		AnnotationSignChars = '+-' # as a class variable @@AnnotationSignChars is not constant
    GenomeAssembly = "hg19"
    NumSamples = 889
    FirstSample = 7

    def initialize(options)
      ts = TS_connector::VirtuosoConnector.new(options)
      exit

			default = {
				:server => 'localhost',
				:port => 10035,
				:base_url => 'http://rdf.biosemantics.org/nanopubs/riken/fantom5/'
			}

			options = default.merge(options)
			super

      @server = AllegroGraph::Server.new(:host=>options[:host], :port=>options[:port],
											   :username=>"agraph", :password=>"agraph")
			@catalog = options[:catalog].nil? ? @server : AllegroGraph::Catalog.new(@server, options[:catalog])
			@repository = RDF::AllegroGraph::Repository.new(:server=>@catalog, :id=>options[:repository])
      uri        = "http://localhost:8890/sparql"
      update_uri = "http://localhost:8890/sparql-auth"
      #@repository = RDF::Virtuoso::Repository.new(uri)
      @repository.clear

      @transcriptCounter = 0
      @sampleInfo = {}
    end

		def convert_header_row(row)
			# do nothing
		end

    # @param [Object] row
    def convert_row(row)
      lineNum = -1

      if row =~ /^chr/
        annotation, shortDesc, description, transcriptAssociation, geneEntrez, geneHgnc, geneUniprot, *samples = row.split("\t")
        #puts row.split("\t").slice(0,7).join("\t")
        @cagePeak = HG19["CagePeak_"+@row_index.to_s]
        insertGraph(nil, [
            [@cagePeak, RDF.type, CAGERES["CagePeak"]],
            [@cagePeak, VOID.inDataset, RDF::URI.new("http://www.riken.jp/data/rdf/fantom5/cage/hg19")]
        ])
        convertAnnotation(annotation)
        convertShortDescription(shortDesc)
        convertDescription(description)
        convertGeneAssoc(shortDesc, geneEntrez, geneHgnc,geneUniprot)
        convertTranscriptAssociation(transcriptAssociation)
        samples.slice(0,NumSamples).each_with_index do |sample, index|
          convertSample(sample, index)
        end
        @row_index += 1
      elsif row =~ /01STAT:MAPPED/
        puts "Found MAPPED stats on line #{lineNum}"
        @sampleInfo["MAPPED"] = row.split("\t").slice(FirstSample,FirstSample+NumSamples)
        #createSamples()
      elsif row =~ /02STAT:NORM_FACTOR/
        puts "Found /NORM_FACTOR stats on line #{lineNum}"
        @sampleInfo["NORM_FACTOR"] = row.split("\t").slice(FirstSample,FirstSample+NumSamples)
        #createSamples()
      elsif row =~ /00Annotation/
        puts "Found Annotations (list of tissues) on line #{lineNum}"
        @sampleInfo["Annotations"] = row.split("\t").slice(FirstSample,FirstSample+NumSamples).map{|tissue| URI.unescape(tissue)}
        #createSamples()
      else
        puts "Unused input line: #{lineNum}"
      end
    end

    def createSamples()
      if @sampleInfo.length == 3
        #puts "Found complete sample info"
        (0..NumSamples).each do |i|
          sample = HG19["Sample_#{i}"]
          insertGraph(nil, [
              [sample, RDF.type, CAGERES.Sample], # add obo type
              [sample, RDFS.label, @sampleInfo["Annotations"][i]],
              [sample, CAGERES.mapped, @sampleInfo["MAPPED"][i]],
              [sample, CAGERES.normFactor, @sampleInfo["NORM_FACTOR"][i]]
          ])
        end
      else
        puts "Did not yet find complete sample info (MAPPED, NORM_FACTOR and Annotation"
      end  
    end
    
    def convertAnnotation(annotation)
      if not annotation =~ /chr(\d+):(\d+)\.\.(\d+),([#{AnnotationSignChars}])/
        puts "Unknown annotation format: ", annotation
        exit
      end
      chromosome, startI, endI, sign = $1, $2, $3, $4
      genRegion = HG19["GenomicRegion_"+@row_index.to_s]
      insertGraph(nil, [
          [@cagePeak, HG19.annotation, genRegion],
          [genRegion, RDF.label, "GenomicRegion"+@row_index.to_s],
          [genRegion, RDF.type, HG19.GenomicRegion],
          [genRegion, CAGERES.chromosome, chromosome],
          [genRegion, CAGERES.genomeAssembly, GenomeAssembly],
          [genRegion, CAGERES.regionStart, startI],
          [genRegion, CAGERES.regionEnd, endI],
          [genRegion, CAGERES.strand, sign]
      ])
      #puts "annotation #{$1}, #{$2}, #{$3}, #{$4}"
    end

    def convertShortDescription(description)
      #puts "shortDesc #{description}"
      if description =~ /,[#{AnnotationSignChars}]/
        #puts "shortDesc is a location annotation"
        insertGraph(nil, [[@cagePeak, CAGERES.shortDescription, description]])
      else
        description.split(",").each do |desc|
          #puts "shortDesc #{desc} added"
          insertGraph(nil, [[@cagePeak, CAGERES.shortDescription, desc]])
        end
      end
    end

    def convertDescription(description)
      #puts "desc #{description}"
      if description =~ /,[#{AnnotationSignChars}]/
        insertGraph(nil, [[@cagePeak, CAGERES.description, description]])
      else
        description.split(",").each do |desc|
          insertGraph(nil, [[@cagePeak, CAGERES.description, desc]])
        end
      end
    end

    def convertToTranscriptId(tA)
      tA.sub(/_\d+end/, "")
      tA.sub(/-?bp_to_/, "")
    end

    def convertTranscriptAssociation(transcriptAssociation)
      if not transcriptAssociation =~ /\ANA\Z/
        #puts "transcriptAssociation #{transcriptAssociation}"
        transcriptAssociation.split(",").each do |tA|
          transcript = HG19["Transcript_"+@transcriptCounter.to_s]
          insertGraph(nil,[
              [transcript, CAGERES.transcriptAssociation, transcript],
              [transcript, RDF.type, HG19.Transcript],
              [transcript, RDFS.label, tA],
              [transcript, DC.id, convertToTranscriptId(tA)]
          ])
          @transcriptCounter += 1
        end
      end
    end

    def geneAssocName(shortDesc)
      parts = shortDesc.split(",").map{|d| d.sub(/^p\d+@/, "")}.uniq
      if shortDesc =~ /p@chr/ or parts.length > 1
        return (0...9).map{65.+(rand(25)).chr}.join.insert(4, '-')
      else
        return parts[0]
      end
    end

    def convertGeneAssoc(shortDesc, geneEntrez, geneHgnc, geneUniprot)
      ids = geneEntrez.split(",")
      if ids.length == 1 and ids[0] != "NA" # suppress creating triples when more than 1 Entrez-geneID
        name = geneAssocName(shortDesc)
        #puts "geneAssociation: \"#{shortDesc}\" gets ID \"#{name}\""
        insertGraph(nil, [
            [@cagePeak, CAGERES.geneAssociation, HG19[name]],
            [HG19[name], RDF.type, CAGERES.Gene],
            [HG19[name], RDFS.label, name],
            [HG19[name], HG19.entrezGeneId, geneEntrez]
        ])
        geneHgnc.split(",").select {|id| id != "NA"}.each {|id| insertGraph(nil, [[HG19[name], CAGERES.hgncId, id]])}
        geneUniprot.split(",").select {|id| id != "NA"}.each {|id| insertGraph(nil, [[HG19[name], CAGERES.uniprotId, id]])}
      else
        #puts "None or multiple EntrezGene IDs in row #{@row_index}"
      end
    end

    def convertSample(sample, sampleNum)
      #puts "sample #{sample}"
      sampleValueString = "Row#{@row_index}Value#{sampleNum}"
      sampleValue = HG19[sampleValueString]
      insertGraph(nil, [
          [@cagePeak, HG19.sampleValue, sampleValue],
          [sampleValue, RDF.type, CAGERES.SampleValue],
          [sampleValue, RDFS.label, sampleValueString],
          [sampleValue, CAGERES.sample, HG19["Sample_#{sampleNum}"]], # TODO add sample objects
          [sampleValue, CAGERES.tpmValue, sample]
      ])
    end
  end

  options = {}
  OptionParser.new do |opts|
    opts.banner = "Usage: fantom5.rb -i data.txt"

    opts.on("-i", "--input ASSEMBLY") do |input|
      options[:input] = input
    end

    opts.on("-o", "--output FILENAME") do |output|
      options[:output] = output
    end

    opts.on("--host HOSTNAME", 'default to localhost') do |host|
      options[:host] = host
    end

    opts.on("--port NUMBER", 'default to 10035') do |port|
      optiosn[:port] = port.to_i
    end

    opts.on("--catalog CATALOGNAME") do |catalog|
      options[:catalog] = catalog
    end

    opts.on("--repository REPOSITORYNAME") do |repository|
      options[:repository] = repository
    end

    opts.on("--base BASEURL") do |base_url|
      options[:base_url] = base_url
    end

    # No argument, shows at tail.  This will print an options summary.
    opts.on_tail("-h", "--help", "Show this message") do
      puts opts
      exit
    end
  end.parse!

  # check for required arguments
  if options[:input].nil?
    puts "input file is missing."
    exit 1
  end

  # do the work
  converter = Nanopublication::Fantom5_RDF_Converter.new(options)
  converter.convert
end
