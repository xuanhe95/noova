# 24fa-cis5550-Noova

### Project Description

This project involves building a fully functional search engine, utilizing components such as a Key-Value Store (KVS), web server, a spark-like flame, a UI, and a crawler. The goal is to ensure seamless data processing, effective web crawling, and responsive user interactions through the web interface.

### Prerequisites
- [Maven](https://maven.apache.org/install.html): Build and dependency management tool.
  ```bash
  # On macOS, you can install Maven using Homebrew
  brew install maven
### How To Run
1. `./start.sh <KVSWorker #> <FlameWorker #>`

OR
1. `mvn clean package`
2. `./run.sh `
3. ```bash
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar" org.noova.kvs.Coordinator 8000
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar" org.noova.kvs.Worker 8001 worker1 localhost:8000
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar" org.noova.flame.Coordinator 9000 localhost:8000
    java -cp "kvs-1.0-SNAPSHOT.jar:generic-1.0-SNAPSHOT.jar:tools-1.0-SNAPSHOT.jar:webserver-1.0-SNAPSHOT.jar:flame-1.0-SNAPSHOT.jar" org.noova.flame.Worker 9001 localhost:9000
    ```

### TODO List

1. **Ensure Key-Value Store (KVS) Functionality**
   - Verify that the KVS operates as expected.
   - Implement conversions between `pt-table` and memory tables to optimize storage and retrieval.

2. **Add Web Server Functionality**
   - Enhance the web server to handle user inputs via webpage forms.
   - Ensure seamless communication between the frontend and backend, enabling effective user interactions with the search engine.

3. **Improve Crawler Efficiency**
   - Optimize the crawler's performance to make it more efficient in retrieving and processing web pages.
   - Address current bottlenecks and refine algorithms to speed up data collection and indexing.
  
4. **我们可以基于session做个用户登陆 然后每个用户基于历史搜索做推荐/个性化排名**
5. **localhost 小bug**
6. 

### Potential Optimization Ideas
- WebServer: use before filter to halt downstream index&rank process for cached query search
### EC Ideas
1. phrase search - add n-grams score
2. autofill/spell check/other nlp-related tasks - [Stanford's CoreNLP library](https://stanfordnlp.github.io/CoreNLP/)
3. Recommendation / surprise me /search suggestion based on mru cached query
4. Infinite scrolling vs Pagination (w/ skips)
5. extracts text content from files such as PDF, DOC(X), XLS(X), etc. - [Apache Tika](https://tika.apache.org/)



## Timeline 
TBD

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
