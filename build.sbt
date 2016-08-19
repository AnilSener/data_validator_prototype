name := "data_validator_prototype"

version := "1.0"

val scalaMajor = "2.11"
val dataCleanerVersion = "5.1.2"
scalaVersion := s"${scalaMajor}.4"
resolvers ++= Seq( "Typesafe Releases" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Maven central" at "http://repo1.maven.org/maven2/")
//TODO: Add a resolver for a lib directory, or unmanagedJars
libraryDependencies ++= {
  Seq(
    "org.eobjects.datacleaner" % "DataCleaner-engine-core" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-api" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-desktop-api" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-engine-utils" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-writers" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-desktop" % dataCleanerVersion pomOnly(),
    "org.eobjects.datacleaner" % "DataCleaner-basic-analyzers" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-testware" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-standardizers" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-basic-filters" % dataCleanerVersion,
    "org.eobjects.datacleaner" % "DataCleaner-uniqueness" % dataCleanerVersion,
    "net.sourceforge.jtds" % "jtds" % "1.3.1",
    "junit" % "junit" % "4.12",
    "org.slf4j" % "slf4j-api" % "1.7.21",
    "org.slf4j" % "slf4j-nop" % "1.7.21"
  )
}

