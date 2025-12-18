import multiprocessing
import unittest
from concurrent.futures import ThreadPoolExecutor

from azul_bedrock import models_restapi

from azul_client import Api
from azul_client.config import _lock_azul_config, get_config

from . import module_ref, module_sha256s, module_source


def setup_api_and_upload_file(content: bytes) -> models_restapi.BinaryData:
    """Create the azul api and upload to Azul.

    This is done so all multiprocesses are interacting with the .azul.ini simultaneously."""
    api = Api(get_config())
    upload_kwargs = {
        "source_id": module_source,
        "filename": "test_multiprocessing_upload",
        "security": "OFFICIAL",
        "references": module_ref,
    }
    resp = api.binaries_data.upload(content, **upload_kwargs)
    print(f"uploaded {content.decode()[:4]}")
    return resp


@_lock_azul_config
def update_azul_ini():
    conf = get_config()
    conf.auth_token_time = 0
    conf.save()


def save_to_azul_init(_index):
    """Method to attempt to modify and update .azul.ini to try and compete with other processes."""
    try:
        for _i in range(4):
            update_azul_ini()
        return "success"
    except Exception as e:
        print(f"Failed to update azul init with error {e}")
        return "fail"


class MultiprocessingAndThreadingUploadTest(unittest.TestCase):

    def test_multiprocessing_upload(self):
        """Verify that multiple files can be simultaneously uploaded to Azul with the azul client."""
        total_files_to_upload = 20
        input_content_list = []
        for input_content in range(total_files_to_upload):
            input_content_list.append(
                (
                    f"{input_content} - Basic and very small amount of content for input"
                    + f" to upload in a multiprocessing context {input_content}" * 20
                ).encode()
            )
        # Perform test
        m_ctx = multiprocessing.get_context("forkserver")
        with m_ctx.Pool(processes=4) as pool:
            output_results = pool.map(setup_api_and_upload_file, input_content_list)

            self.assertEqual(len(output_results), total_files_to_upload)
            # All sha256's should be uploaded.
            output_sha256s = [r.sha256 for r in output_results]
            print(output_sha256s)
            self.assertCountEqual(
                output_sha256s,
                [
                    "325f80b4bd8977075d0581df5d7f2a12d2271487be7d3da2d94536aa615e5aff",
                    "c42840a3fc99138aca7136de0378649c8035a2331dfdb00676c5e76fbddba3f3",
                    "238e269ad74b1b8fd15b4340810029d5f1d054bbef28108097303c9e99b77337",
                    "2bf41b76afab954e4f0569fc0fc223141057eb35e1ed72e29e400d140eb3e1fb",
                    "bf06cde1e4a1c1e4e3ab67c8d99786d5f4784a7b94d5a59a458faac945a4d62d",
                    "382f80b87604dc0eda0052c6a7c5499b93db43af96d27904ed6e0313efa21646",
                    "b5a85307882cbf990760b687841f2138a0eb302e6dbf96ac47a97e351b6bf14c",
                    "8196803e2bdb5fac475a22b592e376dbb71c27ff14cb69d1007378362c1c068b",
                    "bb54f326d59198c7ad41527d4f9754a58f1c3c5fb0b428f740de5e4fc599b7e8",
                    "a0bd01e1a73aa786e2bab6f6022ad72d3bf7e29c87a3879b9a85b2923cd06eec",
                    "247428159ec663342f02d04d8e9792566d16ac013aa3ae88c0677a6fbe52f188",
                    "33915c3b529c8b33495319ea60b1b09e5a331ac0f56721c3e5defb713f6712ff",
                    "ef8b6225b831cf09a736000e6468fdd2cde3c0680481453463735e757755e585",
                    "49e60ab6a45580a94723e05466cb1f89de821b5aa2c99f9e6cb147b44caf03a1",
                    "521f9c8d411ba075927fa40422c177668bbc407d3f15979df244e1230d55e70c",
                    "b10c25c5e8d307df2135f2e9f3794443e3c604f71b26560302c242cb380b24ab",
                    "5439f697ccb7fbb15ba8d2f3720f5ebdc1788a14c449ff39fe8dfaf7a9ffb15a",
                    "a1148fac415968baad75b840a433ad06ed5d27eea9bc4697560adf26667475e1",
                    "e5eb2757cb6f6eab813803f1a72aca0790086702785b86c221247a9365337500",
                    "9097aa562ffa73ce30353cf6f2e40801ac3f4b084750a497e8b6835f7d6693a6",
                ],
            )
            # Verify file formats are all equal
            file_format = list(set(r.file_format for r in output_results))
            self.assertEqual(len(file_format), 1)
            self.assertEqual(file_format[0], "text/plain")
            # Verify mimes are all equal
            file_mime = list(set(r.mime for r in output_results))
            self.assertEqual(len(file_mime), 1)
            self.assertEqual(file_mime[0], "text/plain")

    def test_multiprocessing_modify_azul_ini(self):
        """Have lots of processes simply try and modify .azul.ini and nothing breaks."""
        total_modifications = 20
        m_ctx = multiprocessing.get_context("forkserver")
        with m_ctx.Pool(processes=4) as pool:
            output_results = pool.map(save_to_azul_init, range(total_modifications))
            result = list(output_results)
            self.assertEqual(len(result), total_modifications)
            self.assertEqual(len(set(result)), 1)
            self.assertEqual(result[0], "success")

    def test_threading_upload(self):
        """Verify that multiple files can be simultaneously uploaded to Azul with the azul client."""
        total_files_to_upload = 20
        input_content_list = []
        for input_content in range(total_files_to_upload):
            input_content_list.append(
                (
                    f"{input_content} - Basic and very small amount of content for input"
                    + f" to upload in a threading context {input_content}" * 20
                ).encode()
            )
        # Perform test
        with ThreadPoolExecutor(max_workers=4) as executor:
            output_results = executor.map(setup_api_and_upload_file, input_content_list)
            output_results = list(output_results)

            self.assertEqual(len(output_results), total_files_to_upload)
            # All sha256's should be uploaded.
            output_sha256s = [r.sha256 for r in output_results]
            print(output_sha256s)
            self.assertCountEqual(
                output_sha256s,
                [
                    "a9e59630b94d384de85f38049ebd9fc98c5dc28d0a9a64c9021398d294e3f71d",
                    "1566c0d8e42cf66263fb4241a4d6818afaad72ad5c207d3a5cded4c97e67199e",
                    "3aca772aced98400c1dbda55150bff9a5524a9f2214889e41c75b1e18d0ef710",
                    "f479f8735372dddb2d11a390fc9b871c666e658664641b4991090d6b1f987544",
                    "2a82005c9c66a05ab00e456313381ff25aff0d12649d67b62ae8b8d35f981ad9",
                    "2cb79026088e754eb1f75d67c97ef0199751994ef824c1cea47c72c21af8e77d",
                    "2a56f5d82227d7a8e5bc87c478700315263f933d8a11de8c76ed96852579d914",
                    "cdbfb8e59ba8c65a3918af46f5c4ad0ca6dd161ddfe39a0de3037a7bf18f13e8",
                    "023d17f35aab8284c7735532326d47236c75b602850732edf4f098dc2f008943",
                    "f443f56d63728e058721a5731ce4d0ce2f4603a5bf33f55fb2586bd2b23a026b",
                    "c1134e078894141c1e9e1cf5d223c57bc281453a30d5962c1420cf1b2726f2ce",
                    "76289cd60240c81a9f3408513c514c59b40be8ad8de86462da4563c95406ec49",
                    "4f41d51506d3918ea6c9f537c4cd7f73a356a0b84b5537d71ba9882e06ca6dc2",
                    "89a9fa723a6eb00d897c5a867278092bab39d2f8300fe2a14c4ea06bfe958a9c",
                    "c3802bb20aedc1ad3ba9cabb8cdc9dbb33298f7512348bd36a606c6d9eb97c23",
                    "385b5a2b22c5e721fecf5c26f77ef6ef05d6eccf035e3b022fa2905a107cc001",
                    "b4166b23982b2c41455ddd68058480dc7e9c7ab983c674d4f2630ad99a9df60b",
                    "b5a62ab875dce14c97d4862d78fe35e84ab61cb95332f37d430e5b4f4bdd0ae9",
                    "5471b7bc733de74b73abdc80440307696ea9118ddac772d5fd0f70a35f5b67e1",
                    "5443f2194d2a2d2010022571ffa02a108818d25286efd86f892f56c9dcdf719a",
                ],
            )
            # Verify file formats are all equal
            file_format = list(set(r.file_format for r in output_results))
            self.assertEqual(len(file_format), 1)
            self.assertEqual(file_format[0], "text/plain")
            # Verify mimes are all equal
            file_mime = list(set(r.mime for r in output_results))
            self.assertEqual(len(file_mime), 1)
            self.assertEqual(file_mime[0], "text/plain")

    def test_threading_modify_azul_ini(self):
        """Have lots of threads simply try and modify .azul.ini and nothing breaks."""
        total_modifications = 20
        with ThreadPoolExecutor(max_workers=4) as executor:
            output_results = executor.map(save_to_azul_init, range(total_modifications))
            result = list(output_results)
            self.assertEqual(len(result), total_modifications)
            self.assertEqual(len(set(result)), 1)
            self.assertEqual(result[0], "success")
