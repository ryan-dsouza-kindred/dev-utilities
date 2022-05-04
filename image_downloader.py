import json
import logging
import os
import argparse

import asyncio
import aiohttp

import boto3
from urllib.parse import urlparse


class ImageDownloader:

    def __init__(self, bucket):
        self._s3 = boto3.client('s3')
        self._bucket = bucket
        self._logger = logging.getLogger(self.__class__.__name__)

    def download_images(self, to_downloads, out_dir):
        missing_grasp_ids = []
        for to_download in to_downloads:
            path_component = urlparse(to_download['url']).path
            if not os.path.isfile(os.path.join(out_dir, path_component)):
                missing_grasp_ids.append(to_download)
        self._logger.info(f'Skipping {len(to_downloads) - len(missing_grasp_ids)} grasps '
                          '(already downloaded).')

        loop = asyncio.get_event_loop()
        n_downloaded = loop.run_until_complete(self.download_images_async(
            missing_grasp_ids, out_dir, loop))
        return n_downloaded


    def replacePGID(self, url, to_download) :
        """ Function to remove the pgid in the url with the claimed_slot pgid.
        NOTE the key:[phase_group_id] is the same, even though we are using the slot_id

        ex. removes [e3036eae-eceb-4ac7-8fad-20feeb5c8790] from the following and replaces it with the claimed_slot.slot_id :

        e3036eae-eceb-4ac7-8fad-20feeb5c8790/placement-verification-rgb_downstream-camera_0_1646248099276144000.jpg ->
        6386da62-3537-42cf-9e09-d6dfaa6b2024/placement-verification-rgb_downstream-camera_0_1646248099276144000.jpg

        """
        image_path = url.split("/")
        pgid = to_download['pg_id']
        return os.path.join(pgid,image_path[1])

    async def download_images_async(
        self, to_downloads, out_dir, loop):
        coros = [self._download_images_coro(to_download, out_dir) for to_download in to_downloads]
        await asyncio.gather(*coros)

    async def _download_images_coro(self, to_download, root_dir):
        url = to_download['url']
        image_filename = urlparse(url).path[1:]

        # strip out the pgid from uploaded_media, and use the slot_id from claimed_slots table
        slot_id_image_filename = self.replacePGID(image_filename, to_download)

        image_filepath = os.path.join(root_dir, image_filename)
        slot_id_image_fp = os.path.join(root_dir, slot_id_image_filename)

        if os.path.isfile(image_filepath):
            return

        # os.makedirs(os.path.dirname(image_filepath), exist_ok=True)
        os.makedirs(os.path.dirname(slot_id_image_fp), exist_ok=True)
        try:
            presigned_url: str = self._s3.generate_presigned_url('get_object', Params={
                'Bucket': self._bucket,
                'Key': image_filename,
            })
            async with aiohttp.ClientSession() as session:
                async with session.get(presigned_url) as response:
                    data = await response.read()
                    with open(slot_id_image_fp, 'wb') as f:
                        f.write(data)
        except aiohttp.ClientError as ce:
            self._logger.exception('Encountered ClientError when downloading item', extra={
                'bucket': self._bucket, 'key': image_filename,
                'download_location': slot_id_image_fp, 'exception': ce.__str__(),
            })

    def download(self, to_downloads, output_dir, max_batch_size=100):
        for i in range(0, len(to_downloads), max_batch_size):
            self.download_images(
                to_downloads=to_downloads[i:i+max_batch_size],
                out_dir=output_dir,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser('image_downloader')

    parser.add_argument('--file', help='Path to JSON file with phase_group_id and URL keys', required=True)
    parser.add_argument('--output-path', help='Path to place downloaded files in', required=True)
    parser.add_argument('--s3env', help='Which S3 bucket to use', default='staging', required=False, choices=['production', 'staging'])

    args = parser.parse_args()

    if args.s3env == 'production':
        bucket = 'kin-sms-media-live-ups'
    else:
        bucket = 'kin-sms-media-staging'
    print(bucket)
    downloader = ImageDownloader(bucket)


    # urls = []
    # for line in open(args.file, 'r'):
    #     urls.append(json.loads(line))


    with open(args.file) as f:
        urls = json.load(f)

        downloader.download(urls, args.output_path)

    print('Done')
