# Multi Tier Annotation Search

See [meertensinstituut.github.io/mtas/](https://meertensinstituut.github.io/mtas/)

Or try the docker image providing a demonstration 
scenario with indexing and querying of some sample documents.

```console
docker build -t mtas https://raw.githubusercontent.com/meertensinstituut/mtas/master/docker/Dockerfile
docker run -t -i -p 8080:80 --name mtas mtas
```
