# Twitter Sentiment Analysis with Spark cluster deployment

This script is designed to run with a master and two worker nodes.

## Getting Started

The directions below were tested on a fresh installation of ubuntu 16.04 that has ssh access to 3 machines on digital ocean inc.

### Prerequisites

Please have three virtual machines that you can access via ssh.
First install git in order to clone this directory.
```
sudo apt-get install git
```

Now clone the directory

```
git clone https://github.com/michaelsmith1983/hid-sp18-707.git
```

Run the setup!

```
cd ~/hid-sp18-707/project-code

sudo bash setup.sh
```

Open inventory.txt 

```
vi inventory.txt


```
Add your ip address and usernames in the relevant places to setup the three node cluster, you just a command a way from here.
```

[Master]

<ip address here> ansible_ssh_user=<username here>

[Slave]

<ip address here> ansible_ssh_user=<username here>
<ip address here> ansible_ssh_user=<username here>

```

Run the ansible script and it will deploy spark on your cluster, fetch data, run all scripts.

```
ansible-playbook -i inventory.txt main.yml

```
Monitor the progress of the cluster in your browser and type in the following

```
<your master-ip address>:8080
```
When the analysis is complete, ssh in your master ip address.
```
ssh <username>@<master-ip-address>
```
Once you are logged in move to the spark/bin directory to see output files and images.

```
cd ~/spark-2.3.0-bin-hadoop2.7/bin
```

Some interesting files to see...
```
Overview.txt
acatextcloud.png
polarity_histogram.png
processed_text.txt
twitter_sentiment_list.txt
```

## Authors

Michael Smith 


