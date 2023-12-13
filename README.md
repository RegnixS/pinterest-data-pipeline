# pinterest-data-pipeline238

## Table of Contents
 - [Description](#Description)
 - [Install Instructions](#Install-Instructions)
 - [Usage Instructions](#Usage-Instructions)
 - [File Structure](#File-Structure)
 - [License](#License)

## Description
This project aims to create a similar system processing billions of records similar to Pinterest using AWS Cloud.

An AWS account is provided with the following:
- An EC2 instance
- An MSK cluster


## Install Instructions
Get the URL of this Github repository and pull its contents.

### EC2
Retrieve the key pair for the EC2 instance (in this case from the Parameter Store) and save it locally. \
This allows access to the EC2 instance using SSH.

### Kafka Client
The following components are installed on the EC2 instance:
- Java 1.8.0
- Kafka 2.12-2.8.1
- IAM MSK Authentication package

The path to the IAM MSK Authentication package's .jar file must be added to CLASSPATH in .bashrc. \
An IAM role for the EC2 instance to access MSK needs to be configured and added to the client.properties file in the Kafka*/bin directory.

The following Kafka topics are created:
- 129a67850695.pin
- 129a67850695.goe
- 129a67850695.user






## Usage instructions
### Python files
user_posting_emulation.py : This file at random intervals, pulls random data from a mySQL database. \
This consists of a set of three datasets which are:
- details of the 'pin'
- details of a location from where this 'pin' originated
- details of a user who posted the 'pin' 


## File structure of the project:
user_posting_emulation.py : generates source data emulating users of the pinterest application.

## License information:
MIT License

Copyright (c) 2023 Robert Ducke

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
