﻿# 24fa-cis5550-Noova

### Project Description

This project involves building a fully functional search engine, utilizing components such as a Key-Value Store (KVS), web server, a spark-like flame, a UI, and a crawler. The goal is to ensure seamless data processing, effective web crawling, and responsive user interactions through the web interface.

### Prerequisites
- [Maven](https://maven.apache.org/install.html): Build and dependency management tool.
  ```bash
  # On macOS, you can install Maven using Homebrew
  brew install maven

### Libaray used
JQuery https://jquery.com/
Bootstrap https://getbootstrap.com/

### How To Run
1. Run maven command to create the jar for each component\
    `./build.sh`
2. Start coordinators and workers in Flame and KVS\
   `./start.sh <KVSWorker #> <FlameWorker #>`

OR
 ```bash
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar" org.noova.kvs.Coordinator 8000
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar" org.noova.kvs.Worker 8001 worker1 localhost:8000
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar" org.noova.flame.Coordinator 9000 localhost:8000
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar" org.noova.flame.Worker 9001 localhost:9000
```
3. Run Crawler/Pagerank/Indexer
```bash
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar" org.noova.flame.FlameSubmit localhost:9000 crawler-1.0-SNAPSHOT.jar  org.noova.crawler.Crawler http://simple.crawltest.cis5550.net/
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar" org.noova.flame.FlameSubmit localhost:9000 indexer-1.0-SNAPSHOT.jar org.noova.indexer.Indexer
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar" org.noova.flame.FlameSubmit localhost:9000 pagerank-1.0-SNAPSHOT.jar org.noova.pagerank.PageRank 0.01
 ```

## Team members
- Full Name: Yuan Ding  
   SEAS Login: yding42@seas.upenn.edu  
   Github User ID: yding42up

- Full Name: Zhe Huang  
   SEAS Login: zhehuang@seas.upenn.edu  
   Github User ID: ZeeJJ123

- Full Name: Ying Zhang  
   SEAS Login: yzhang9@seas.upenn.edu  
   Github User ID: athrala

- Full Name:  Xuanhe Zhang  
   SEAS Login: xuanhe@seas.upenn.edu  
   Github User ID: xuanhe95

## Crawler URL start point
https://www.wikipedia.org
https://www.cnn.com
https://www.bbc.com
https://www.reddit.com
https://www.imdb.com
https://stackoverflow.com
https://www.allrecipes.com

## Git Strategy

### Branching Strategy

- **Main Branch** (`main`):  
  This is the production-ready branch. All code in this branch should be tested, stable, and deployable. Direct commits to this branch are restricted. Only pull requests that have passed review and testing should be merged here.

- **Feature Branches** (`feature/your-feature-name`):  
  For new features, create a branch from the main branch. Naming convention: `feature/short-description` (e.g., `feature/user-authentication`). Merge into the `main` branch only after code review and approval.

- **Bugfix Branches** (`bugfix/short-description`):  
  Use bugfix branches for resolving issues in the main branch. Naming convention: `bugfix/short-description` (e.g., `bugfix/login-error`). Merge into `main` once reviewed and tested.

- **Hotfix Branches** (`hotfix/short-description`):  
  For urgent fixes in the main branch, use hotfix branches. Naming convention: `hotfix/short-description` (e.g., `hotfix/critical-security-patch`). Hotfix branches are merged into both `main` and other relevant branches.

### Pull Request Process

1. **Sync and Pull Latest Changes**:  
   Always pull the latest changes from `main` into your branch before starting a new feature or bugfix.

2. **Create a Pull Request (PR)**:  
   When ready, create a PR from your feature or bugfix branch to `main`. Provide a clear description of the changes and any necessary context.

3. **Request Review**:  
   Add reviewers to your PR. Await feedback and address any requested changes.

4. **Squash and Merge**:  
   Once approved, the PR will be merged using the “squash and merge” strategy, consolidating your branch into a single commit in the `main` branch.

5. **Delete Branch**:  
   After merging, delete your feature or bugfix branch to keep the repository tidy.
