# Poseidon: Distributed Deep Learning Framework on Petuum

Poseidon is a scalable open-source framework for large-scale distributed deep learning on CPU/GPU clusters. It is initially released in July 2015 along with PetuumV1.1 as an app under the **Bösen** parameter server.

Poseidon builds upon the Caffe (http://caffe.berkeleyvision.org/) CNN libraries and the Petuum distributed ML framework (http://petuum.github.io/) as a starting point, but goes further by implementing three key contributions for efficient CNN training on clusters of GPU-equipped machines: (i) a three-level hybrid architecture that allows Poseidon to support both CPU-only clusters as well as GPU-equipped clusters, (ii) a distributed wait-free backpropagation (DWBP) algorithm to improve GPU utilization and to balance communication, and (iii) a dedicated structure-aware communication protocol (SACP) to minimize communication overheads.

Poseidon's design philosophy is rooted on efficiently harnessing multiple, distributed GPUs on commodity hardware and Ethernet, in order to maximize the speedup with a fully data parallel scheme for distributed deep learning. We empirically evaluate Poseidon regarding of throughput, convergence and accuracy on the image classification tasks with multiple standard datasets, and show that Poseidon is able to achieve state-of-the-art speedups in accelerating the training of modern CNN structures, at the same time guarantee the correct convergence. 

Poseidon inherits many functionalities and benefits of Petuum, including the Sufficient Factor Broadcasting (SFB) [1], managed communication and bandwidth management in the [Bösen parameter server](https://github.com/petuum/bosen) [2], etc. Moreover, most of the Caffe interfaces are kept unchanged.

Please consult the [wiki page](https://github.com/petuum/poseidon/wiki) for more details on how to setup Poseidon on your clusters and start training your model.

[1] Pengtao Xie, Jin Kyu Kim, Yi Zhou, Qirong Ho, Abhimanu Kumar, Yaoliang Yu, and Eric Xing. Distributed Machine Learning via Sufficient Factor Broadcasting. In arXiv, 2015. 

[2] Jinliang Wei, Wei Dai, Aurick Qiao, Henggang Cui, Qirong Ho, Gregory R Ganger, Phillip B. Gibbons, Garth A. Gibson, and Eric Xing. Managed Communication and Consistency for Fast Data-Parallel Iterative Analytics. In SoCC, 2015.


