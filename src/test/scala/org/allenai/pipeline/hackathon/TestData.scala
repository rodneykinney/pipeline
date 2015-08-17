package org.allenai.pipeline.hackathon

object TestData {
  import CommandToken._
  import java.net.URI

  val script = Pipescript(
    packages = Seq(
      Package(id = "scripts", source = new URI("./vision-py/scripts"))
    ),
    runCommands = Seq(
      RunCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractArrows.py"),
          StringToken("-i"),
          InputDir(source = new URI("./vision-py/png")),
          StringToken("-o"),
          OutputDir("arrowDir")
        )
      ),
      RunCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractBlobs.py"),
          StringToken("-i"),
          InputDir(source = new URI("./vision-py/png")),
          StringToken("-o"),
          OutputDir("blobsDir")
        )
      ),
      RunCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractText.py"),
          StringToken("-i"),
          InputDir(source = new URI("./vision-py/png")),
          StringToken("-o"),
          OutputDir("textDir")
        )
      ),
      RunCommand(
        Seq(
          StringToken("python"),
          PackagedInput("scripts", "ExtractRelations.py"),
          StringToken("-a"),
          ReferenceOutput("arrowDir"),
          StringToken("-b"),
          ReferenceOutput("blobsDir"),
          StringToken("-t"),
          ReferenceOutput("textDir"),
          StringToken("-o"),
          OutputDir("relationsDir")
        )
      )
    )
  )
}
