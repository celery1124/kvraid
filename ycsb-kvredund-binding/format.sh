#!/bin/bash

# format kvssd
nvme format /dev/nvme0n1
nvme format /dev/nvme1n1
nvme format /dev/nvme2n1
nvme format /dev/nvme3n1
nvme format /dev/nvme4n1
nvme format /dev/nvme5n1

echo kvssd format completed
