This sub-project is used for running quick tests without Pig running on Hadoop. The server used here simulates HTTP requests that will be sent by Pig. To configure the executing simulation, those following files should be correctly written:
  1. config.txt:
    This file should contain all operation meta information, where each line follows the format: "<Srcvars>;<Operation>;<Detvars>". Also, the writing order should be organised as the real exetution order. That means operations cannot be listed after those who is dependent on them.
  2. EvalBeforeTerm:
    This file should contain all variable names that're going to be evaluated before the pipeline is terminated. Each line contains only one name string.
  3. EvalAfterTerm:
    This file should contain all variable names that're going to be evaluated after the pipeline is terminated. Each line contains only one name string.
  4. result.csv:(No need to modify)
    This file is the result table of all components in the pipeline. Meta information of each step can be viewed here clearly.
