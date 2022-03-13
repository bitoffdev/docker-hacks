from collections import namedtuple
import gzip
import hashlib
import io
import json
import tarfile
import tempfile

from dxf import DXF


def dxf_get_blob_bytesio(_dxf: DXF, digest):
    _io = io.BytesIO()
    for chunk in _dxf.pull_blob(digest):
        _io.write(chunk)
    return _io
def dxf_download_blob_to_tmp(_dxf: DXF, digest) -> str:
    mytemp = tempfile.NamedTemporaryFile(delete=False, mode="wb")
    for chunk in _dxf.pull_blob(digest):
        mytemp.write(chunk)
    mytemp.close()
    return mytemp.name


def dxf_write_blob_to_fh(_dxf: DXF, digest, fh) -> None:
    for chunk in _dxf.pull_blob(digest):
        fh.write(chunk)


def tar_write_from_io(archive, relative_path, fobj):
    # store the current position in the file handle so we may return to it later
    start_pos = fobj.tell()
    # seek to the end of the file and get the location
    fobj.seek(0, 2)
    end_pos = fobj.tell()
    # return to the former position in the file to start reading
    fobj.seek(start_pos)
    # determine the number of bytes that we will write
    size = end_pos - start_pos
    
    # build up the required tarinfo for the file we're adding
    info = tarfile.TarInfo(name=relative_path)
    # the "size" attribute of the TarInfo will be used to determine how
    # many bytes are read from `fileobj`, so it's quite important
    info.size = size

    archive.addfile(info, fobj)


def tar_write_bytes(archive, relative_path, _bytes):
    return tar_write_from_io(archive, relative_path, io.BytesIO(_bytes))


def dxf_get_blob_bytesio(_dxf: DXF, digest):
    _io = io.BytesIO()
    for chunk in _dxf.pull_blob(digest):
        _io.write(chunk)
    _io.seek(0)
    return _io


def dxf_get_blob_bytes(_dxf: DXF, digest: str) -> bytes:
    _io = dxf_get_blob_bytesio(_dxf, digest)
    try:
        return _io.read()
    finally:
        _io.close()
###########################################################

Layer = namedtuple("Layer", ["tarball_io", "tarball_digest", "gzipped_tarball_io", "gzipped_tarball_digest"])

def make_hello_world_layer() -> Layer:
    layer_tarball_io = io.BytesIO()
    layer_tf = tarfile.TarFile.open(fileobj=layer_tarball_io, mode='w')
    tar_write_bytes(layer_tf, "hello_world.py", b"print('Hello World!')")
    layer_tf.close()
    layer_tarball_io.seek(0)
    layer_tarball_digest = f"sha256:{hashlib.sha256(layer_tarball_io.read()).hexdigest()}"

    layer_gzipped_tarball_io = io.BytesIO()
    layer_gz = gzip.GzipFile(fileobj=layer_gzipped_tarball_io, mode='w')
    layer_tarball_io.seek(0)
    layer_gz.write(layer_tarball_io.read())
    layer_gz.close()
    layer_gzipped_tarball_io.seek(0)
    layer_gzipped_tarball_digest = f"sha256:{hashlib.sha256(layer_gzipped_tarball_io.read()).hexdigest()}"

    return Layer(layer_tarball_io, layer_tarball_digest, layer_gzipped_tarball_io, layer_gzipped_tarball_digest)


def append_hello_world_layer(new_image_name, base_image_name):
    base_repository, base_tag = base_image_name.rsplit(":", 1)
    new_repository, new_tag = new_image_name.rsplit(":", 1)

    # We cannot skip uploading prior layers if the new image will not get
    # uploaded to the same repository!
    assert new_repository == base_repository
    # Prevent us from overwritting the old tag.
    assert new_tag != base_tag

    host, path = base_repository.split("/", 1)
    # create a Docker repo object
    dxf = DXF(host, path, insecure=True)
    # fetch the manifest for the base image
    manifest = json.loads(dxf.get_manifest(base_tag))

    # make a new layer that just has a hello world Python script
    layer = make_hello_world_layer()
    # Push the layer
    dxf.push_blob(data=layer.gzipped_tarball_io.getvalue(), digest=layer.gzipped_tarball_digest)

    # Get the runtime container config from the base image manifest
    config = json.loads(dxf_get_blob_bytes(dxf, manifest["config"]["digest"]).decode('utf-8'))
    # Add the layer's (unzipped) tarball digest to the diff_ids
    config["rootfs"]["diff_ids"].append(layer.tarball_digest)
    # Specify the Dockerfile command that was supposedly used to generate the
    # new layer
    config["history"].append({
        "created": "2021-11-24T20:19:40.483367546Z",
        "created_by": "#(nop) COPY file:22f2428e67dfee2ec4b25337f1d7eaea8398e73d5de7bbe5a08f9ef5b2756325 in /hello_world.py",
    })
    # Upload the config file as a blob to the Docker repo
    config_bytes = json.dumps(config).encode("utf-8")
    config_digest = f"sha256:{hashlib.sha256(config_bytes).hexdigest()}"
    dxf.push_blob(data=config_bytes, digest=config_digest)

    # Update the manifest to list our *gzipped* layer
    manifest['layers'].append({
        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
        "size": len(layer.gzipped_tarball_io.getvalue()),
        "digest": layer.gzipped_tarball_digest,
    })
    # Update the manifest to use our newly uploaded config
    manifest["config"] = {
        'mediaType': 'application/vnd.docker.container.image.v1+json',
        "size": len(config_bytes),
        "digest": config_digest,
    }
    # Push the new manifest to our new tag, completing the "image push"
    dxf.set_manifest(new_tag, json.dumps(manifest))


append_hello_world_layer(
    "localhost:5000/my-alpine:new-tag-lol",
    "localhost:5000/my-alpine:latest",
)

