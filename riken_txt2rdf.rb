require 'rubygems'
require 'rdf/turtle'
require 'rdf/trig'
require 'rdf/rdfxml'

#RDF::Reader.open("/Users/mark/Downloads/rdf/np-new.trig") do |reader|
#  reader.each_statement do |statement|
#    puts statement
#  end
#end

#g2 = RDF::Graph.new(RDF::URI.new("http://assertion.bla"))
#
#s = RDF::Statement.new(RDF::URI.new("http://www.bla.org"),
#                       RDF::DC.creatorr,
#                       RDF::Literal.new("hello"),
#                       :context => RDF::URI.new("http://assertion.bla"))
#s2 = RDF::Statement.new(RDF::URI.new("http://www.nanopub.org"),
#                        RDF::DC.creator,
#                        RDF::Literal.new("bladibla"),
#                        :context => RDF::URI.new("http://assertion.bla"))
#g.insert(s)
#g2.insert(g)
#g2.insert(s2)

AnnotationSignChars = '+-'
HG19 = RDF::Vocabulary.new("http://www.riken.jp/data/rdf/fantom5/cage/hg19/")
VOID = RDF::Vocabulary.new("http://rdfs.org/ns/void#>")
RDFS = RDF::Vocabulary.new("http://www.w3.org/2000/01/rdf-schema#")
GenomeAssembly = "GenomeAssembly"
Dataset = RDF::Graph.new(RDF::URI.new("http://riken.org"))
$transcriptCounter = 0

def storeTriple(s, p, o)
  Dataset.insert( RDF::Statement.new(s, p, o) )
end

# @param [RDF::Resource] rowRDF
# @param [Integer] rowNum
# @param [String] annotation
def convertAnnotation(rowRDF, rowNum, annotation)
  if not annotation =~ /chr(\d+):(\d+)\.\.(\d+),([#{AnnotationSignChars}])/
    puts "Unknown annotation format: ", annotation
    exit
  end
  chromosome, startI, endI, sign = $1, $2, $3, $4
  genRegion = HG19["GenomicRegion_"+rowNum.to_s]
  storeTriple(rowRDF, HG19.annotation, genRegion)
  storeTriple(genRegion, RDF.label, "GenomicRegion"+rowNum.to_s)
  storeTriple(genRegion, RDF.type, HG19.GenomicRegion)
  storeTriple(genRegion, HG19.chromosome, chromosome)
  storeTriple(genRegion, HG19.genomeAssembly, GenomeAssembly)
  storeTriple(genRegion, HG19.start, startI)
  storeTriple(genRegion, HG19.end, endI)
  storeTriple(genRegion, HG19.strand, sign)

  puts "annotation #{$1}, #{$2}, #{$3}, #{$4}"
end

def convertShortDescription(rowRDF, rowNum, description)
  puts "shortDesc #{description}"
  if description =~ /,[#{AnnotationSignChars}]/
    storeTriple(rowRDF, HG19.description, description)
  else
    description.split(",").each do |desc|
      storeTriple(rowRDF, HG19.description, desc)
    end
  end
end

def convertDescription(rowRDF, rowNum, description)
  puts "desc #{description}"
  if description =~ /,[#{AnnotationSignChars}]/
    storeTriple(rowRDF, HG19.description, description)
  else
    description.split(",").each do |desc|
      storeTriple(rowRDF, HG19.description, desc)
    end
  end
end

def convertToTranscriptId(tA)
  tA.sub(/_\d+end/, "")
  tA.sub(/-?bp_to_/, "")
end

# @param [RDF::Resource] row
# @param [Integer] rowNum
# @param [String] transcriptAssociation
def convertTranscriptAssociation(row, rowNum, transcriptAssociation)
  if not transcriptAssociation =~ /\ANA\Z/
    puts "transcriptAssociation #{transcriptAssociation}"
    transcriptAssociation.split(",").each do |tA|
      transcript = HG19["Transcript_"+$transcriptCounter.to_s]
      storeTriple(row, HG19.transcriptAssociation, transcript)
      storeTriple(transcript, RDF.type, HG19.Transcript)
      storeTriple(transcript, RDFS.label, tA)
      storeTriple(transcript, HG19.id, convertToTranscriptId(tA))
      $transcriptCounter += 1
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

def convertGeneAssoc(row, rowNum, shortDesc, geneEntrez, geneHgnc, geneUniprot)
  ids = geneEntrez.split(",")
  if ids.length == 1 and ids[0] != "NA" # no triples when more than 1 Entrez-geneID
    name = geneAssocName(shortDesc)
    puts "geneAssociation: \"#{shortDesc}\" gets ID \"#{name}\""
    storeTriple(row, HG19.geneAssociation, HG19[name])
    storeTriple(HG19[name], RDF.type, HG19.Gene)
    storeTriple(HG19[name], RDFS.label, name)
    storeTriple(HG19[name], HG19.entrezGeneId, geneEntrez)
    geneHgnc.split(",").select {|id| id != "NA"}.each {|id| storeTriple(HG19[name], HG19.hgncId, id)}
    geneUniprot.split(",").select {|id| id != "NA"}.each {|id| storeTriple(HG19[name], HG19.uniprotId, id)}
  else
    puts "None or multiple EntrezGene IDs in row #{rowNum}"
  end
end

def convertSample(rowRDF, rowNum, sample, sampleNum)
  puts "sample #{sample}"
  sampleValueString = "Row#{rowNum}Value#{sampleNum}"
  sampleValue = HG19[sampleValueString]
  storeTriple(rowRDF, HG19.sampleValue, sampleValue)
  storeTriple(sampleValue, RDF.type, HG19.SampleValue)
  storeTriple(sampleValue, RDFS.label, sampleValueString)
  storeTriple(sampleValue, HG19.sample, HG19["Sample_#{sampleNum}"]) # TODO add sample objects
  storeTriple(sampleValue, HG19.tpmValue, sample)
end

# @param [String] line
# @param [Integer] rowNum
def convertRow(line, rowNum)
  annotation, shortDesc, description, transcriptAssociation, geneEntrez, geneHgnc, geneUniprot, *samples = line.split("\t")

  puts line.split("\t").slice(0,7).join("\t")
  rowRDF = HG19["Row_"+rowNum.to_s]
  storeTriple(rowRDF, RDF.type, HG19.Row)
  storeTriple(rowRDF, VOID.inDataset, RDF::URI.new("http://www.riken.jp/data/rdf/fantom5/cage/hg19"))

  convertAnnotation(rowRDF, rowNum, annotation)

  convertShortDescription(rowRDF, rowNum, shortDesc)

  convertDescription(rowRDF, rowNum, description)

  convertGeneAssoc(rowRDF, rowNum, shortDesc, geneEntrez, geneHgnc,geneUniprot)

  convertTranscriptAssociation(rowRDF, rowNum, transcriptAssociation)

  samples.slice(0,3).each_with_index do |sample, index|
    convertSample(rowRDF, rowNum, sample, index)
  end

end

inputFile = '/Users/mark/riken/tc.decompose_smoothing_merged.ctssMaxCounts11_ctssMaxTpm1.tpm.selected.clustername_update.desc.osc.short.txt'

rowCount = 0
lineNum = 0
NumSamples = 889
sampleInfo = {}
File.open(inputFile, "r") do |infile|
  while(line = infile.gets)
    if line =~ /^chr/
      convertRow(line, rowCount)
      rowCount += 1
    elsif line =~ /01STAT:MAPPED/
      puts "Found MAPPED stats on line #{lineNum}"
      sampleInfo["MAPPED"] = line.split("\t").slice(7,7+NumSamples)
    elsif line =~ /02STAT:NORM_FACTOR/
      puts "Found /NORM_FACTOR stats on line #{lineNum}"
      sampleInfo["NORM_FACTOR"] = line.split("\t").slice(7,7+NumSamples)
    elsif line =~ /00Annotation/
      puts "Found Annotations (list of tissues) on line #{lineNum}"
      sampleInfo["Annotations"] = line.split("\t").slice(7,7+NumSamples).map{|tissue| URI.unescape(tissue)}
    else
      puts "Unused input line: #{lineNum}"
    #break
    end
    lineNum += 1
  end
end

if(sampleInfo.length == 3)
  puts "Found complete sample info"
  (0..3).each do |i|
    sample = HG19["Sample_#{i}"]
    storeTriple(sample, RDF.type, HG19.Sample)
    storeTriple(sample, RDFS.label, sampleInfo["Annotations"][i])
    storeTriple(sample, HG19.mapped, sampleInfo["MAPPED"][i])
    storeTriple(sample, HG19.normFactor, sampleInfo["NORM_FACTOR"][i])
  end
else
  puts "Did not find complete sample info (MAPPED, NORM_FACTOR and Annotation"
end

#RDF::TriG::Writer.buffer do |writer|
#  Dataset.each_statement do |statement|
#    writer << statement
#    puts writer
#  end
#end

#puts RUBY_ENGINE + RUBY_VERSION

prefixes = {
    :hg19 => HG19,
    :rdf => RDF,
    :rdfs => RDFS,
    #:riken => RDF::Vocabulary.new("http://riken.org")
}
#RDF::RDFXML::Writer.initialize($stdout, { :base_uri => "http://example.com/" })

#thing = RDF::TriG::Writer.buffer(:file_extension => "trig", :prefixes => {
#    nil => "http://riken.org/",
#    :hg19 => "http://www.riken.jp/data/rdf/fantom5/cage/hg19/",
#    :rdfs => "http://rdfs.org/ns/void#"})
RDF::TriG::Writer.open("testout.rdf") do |writer|
  writer.prefixes = prefixes
  writer.base_uri = "http://riken.org"
  writer << Dataset
end

# only writes: #<RDF::NTriples::Writer:0x2dca4eb4>
#writer = nil
#RDF::Writer.for(:ntriples).buffer do |writer|
#  Dataset.each_statement do |s|
#    writer << s
#  end
#end
#puts writer