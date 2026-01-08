import glob
from tempfile import NamedTemporaryFile
from contextlib import contextmanager

import awswrangler as wr
import boto3
from smart_open.s3 import parse_uri
from joblib import register_store_backend
from joblib._store_backends import StoreBackendBase, StoreBackendMixin, CacheItemInfo


class S3StoreBackend(StoreBackendBase, StoreBackendMixin):
    _item_exists = staticmethod(wr.s3.does_object_exist)

    @contextmanager
    def _open_item(self, location, mode):
        # For simplicity, we only support the modes that joblib uses.
        if mode == "rb":
            with NamedTemporaryFile("rb") as f:
                # Translate the exception to something that joblib
                # understands.
                try:
                    wr.s3.download(location, f.name)
                except Exception:
                    raise FileNotFoundError(location)
                yield f
        elif mode == "wb":
            with NamedTemporaryFile("w+b") as f:
                yield f
                f.seek(0)
                wr.s3.upload(f, location)
        else:
            raise ValueError("mode must be 'rb' or 'wb'")

    def _move_item(self, src_uri, dst_uri):
        # awswrangler only includes a fancy move/rename method that actually
        # makes it pretty hard to just do a simple move.
        src, dst = [parse_uri(x) for x in (src_uri, dst_uri)]
        self.client.copy_object(
            Bucket=dst["bucket_id"],
            Key=dst["key_id"],
            CopySource=f"{src['bucket_id']}/{src['key_id']}",
        )
        self.client.delete_object(Bucket=src["bucket_id"], Key=src["key_id"])

    def create_location(self, location):
        # Actually don't do anything. There are no locations on S3.
        pass

    def clear_location(self, location):
        # Recursive delete.
        wr.s3.delete_objects(glob.escape(location))

    def get_items(self):
        return [
            CacheItemInfo(
                key,
                item["ContentLength"],
                # This is supposed to be an access date, but it only gets used
                # this way for LRU caching, so it's not a big deal to use the
                # modified time instead.
                item["LastModified"],
            )
            for key, item in wr.s3.describe_objects(self.location).items()
        ]

    def configure(self, location, verbose=0, backend_options={}):
        self.verbose = verbose
        self.compress = backend_options.get("compress", False)

        self.mmap_mode = backend_options.get("mmap_mode")
        if self.mmap_mode is not None:
            raise ValueError("impossible to mmap on S3.")

        if not location.startswith("s3://"):
            raise ValueError("location must be an s3:// URI")

        # Theoretically we should validate that the bucket exists and we have
        # permission to write files to it, but for the POC I'm lazy.
        self.location = location

        # Also create a boto3 client that gets its configuration from awswrangler.
        self.client = boto3.Session().client(
            "s3", endpoint_url=wr.config.s3_endpoint_url
        )


def install():
    register_store_backend("s3", S3StoreBackend)
