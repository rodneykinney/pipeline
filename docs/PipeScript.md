PipeScript
===

PipeScript is a simple scripting language that makes your workflows reproducible and sharable.

A PipeScript file can execute an arbitrary sequence of commands. After executing them, it will

- Upload all inputs to a shared location
- Write all outputs to a shared location
- Create a portable script that any user can run from anywhere to exactly reproduce the results
- Create an HTML page with a visualization of the workflow, with links to all input, intermediate, and final data 

Suppose you have a shell script named `ProcessData.sh` that takes an input file (specified
with an `--input` argument) and produces an output file (specified with an `--output` argument).
The equivalent PipeScript file would be:
 
    run {input: ProcessData.sh} --input {input: InputData.txt} --output {output: result}
     
When PipeScript executes this script, it will upload each input to the output location, using
a file name that includes a checksum of the file contents. Later, if you change 
the contents of `ProcessData.sh` or `InputData.txt` and rerun the script, new copies of the inputs will be upload, but
the old versions will still be retained. Thus, a complete history of all past PipeScript run
is kept in the output location, allowing any past result to be reproduced.

You can run a PipeScript file with  

    pipeScript/bin/runPipeScript <PipeScript-file> <output-location-url>
    
where `output-location-url` can be a `file:` or `s3:` URL.  
