# Multi Tier Annotation Search

See [meertensinstituut.github.io/mtas/](https://meertensinstituut.github.io/mtas/)
or build and run a Docker image providing a demonstration 
scenario with indexing and querying of some sample documents.

```console
docker build -t mtas https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/Dockerfile
docker run -t -i -p 8080:80 --name mtas mtas
```

This will provide a website on port 8080 on the ip of your docker host.
