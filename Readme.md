# Project Overview 

	1. Download files 
	2. As per requirement downloded files generate metadata
	3. Written in pyspark 
	4. Approx execution time < 2~5 min 



## How to build Docker 


	1. mkdir docker_test_v1
	2. cd docker_test_v1
	3. git clone https://github.com/alokd31999/gen_files_meta_data.git
	4. cd gen_files_meta_data
	5. docker build --no-cache -t pyspark_files_metadata_v1 .

## How to run Docker

	1. docker run -it --name "cs-sample_file_0-sample_file_19-file-metadata" pyspark_files_metadata_v1

## How to see docker info
    1. docker ps -a # get running CONTAINER ID
    2. docker stop <CONTAINER ID  f7cd0e227d47> # stop CONTAINER ID
    3. docker rm <CONTAINER ID  f7cd0e227d47> # remove CONTAINER ID
## How to test container

	1. parquet-tools show hashes.pq
	2. parquet-tools show data.pq
	3. execution log in : /app/logs

Out put should be root@f7cd0e227d47:/app# parquet-tools show hashes.pq
+------------------------------------------------------------------+
| sha256_hexdigest                                                 |
|------------------------------------------------------------------|
| 89abab31bc4c08205ea4190cac98deb0b9844da121acff9de93ea41adade8a75 |
| b81c412ac9325e659bc917396aca3e4439c4cc4c1d695eca13e29fef4e14be32 |
| 0d3dfaf6acad01f789b7135a3c9ffde4317b9699b8e4e5b7c0c950e90f66bc32 |
| 024cb8086e8a60430e32441e51a2c233d423e587c0e40638a5f71f4d574ade01 |
| 6bbdf35d89938832aff331d037167dde820edcffae28d22e0f32207906edcb6a |
| d9e91ca2a69c0730eeb88b73fbc55cb27efa8003eae7fbeb15ae233374c0dc67 |
| df66f4b475c0176a7b5465f1a538adb835bdb9e87163c1b1df3e9a3c5cb62659 |
| 8f5fe52e82c5595d4d606567bc7f0cdd209ac590b014cf220a76a3c9609c4738 |
| 3a5144b00d509bc0f1bde0dea3d0a12412dbbaba141a975ba5ed03436c3109fd |
| 279d88870a9c0c5163028250f62e9b561a9ec07e28e1f5964f656fcad7ee1970 |
| 3bdc37911ab4707b6bebe0b1932b00099afc810a21e08859507579602281bc9a |
| d0038f1688bef50aeb01996efbb8c4337315d192472579638f6b09e929bfeea1 |
| 408d38a9efe53ae52480e31f8b8867cf2822e4ee1b91b6b276b036545bf89d65 |
| 3fc3d82f8bf6c0a00563f4fe9d246165cf06bc01b04be8ddf0961b3d56e84027 |
| acdffc7d7f44632359d5a5f6b464ee5fbb640016786235201123ead28ff4ba4e |
| e91b870220dd6d9baa48071a885ffa451784e4c4d193b94d0c1b3df4795373ad |
| c3ff5e424e328b45f22ed062cd577751f55362d2e354bd19a55636fceaed3de1 |
| c739b474738fba95654dcb638eb810860d14fb7dc7c9a7642f7f402517805179 |
| 46ceb13da40a7885924b635938f921c656ff865a759b4af4a40d7d6c7360766d |
| 7ec7b87b5fd618f6dd6f1539be208e2013487c52baf60580399c767289055bf7 |
+------------------------------------------------------------------+

Out put should be root@f7cd0e227d47:/app# parquet-tools show data.pq
+--------------------+-------------+--------------+---------------+--------------+
| file_name          |   file_size |   word_count |   unique_word | today_date   |
|--------------------+-------------+--------------+---------------+--------------|
| sample_file_0.txt  |      371240 |        32025 |         29449 | 2021-02-01   |
....
....
## Git local auth with username and password
    1. git remote set-url origin https://username:password@github.com/alokd31999/gen_files_meta_data.git
    2. Note : This is public repo so other users not need any login and password to clone