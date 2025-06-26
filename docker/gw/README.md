# MinIO configuration

1. Log in to MinIO at http://localhost:9001 and create a bucket

2. Connect to the MinIO container 
```bash
docker exec -it <containerId> /bin/bash
```

3. Set credentials
```bash
mc alias set local http://localhost:9000 admin adminadmin
```

4. Register backend
```bash
mc ilm tier add s3 local AURORA --endpoint http://host.docker.internal:8078 --bucket default --access-key foo --secret-key foo
```

5. Add a tier transition rule
```bash
mc ilm rule add local/<bucket_name> --transition-tier AURORA --transition-days 0
```