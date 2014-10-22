#Distributed External Sort
=======================

Distributed External Sort in Java. This is currently in development phase. The code repository will be constantly updated with new code.
We will also try to comeup with some action plan and possible design.

Currently this code is in development phase and will have lot of our local environment specific details. Please change those while trying to test. We are at very initial phase of development so we are not putting efforts to change those.

## Credits and thanks
This is going to be distributed implementation of [External Sorting in Java 8](https://github.com/lemire/externalsortinginjava8).
For distributed architecture we are using [JPPF](http://www.jppf.org/), which helps to distribut the process in tasks across cluster. We preferred JPPF as it is very easy to configure and deploy without much overhead even during execution. The deployment details with configurations can be located at [JPPF link](http://www.jppf.org/doc/v4/index.php?title=Main_Page).

## Licence 
This repository is released under [Apache 2 license](https://github.com/wkapil/distributedexternalsort/blob/master/LICENSE).
