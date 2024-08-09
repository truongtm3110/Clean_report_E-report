import os

from apprise import AppriseAsset, Apprise
from apprise.plugins import SCHEMA_MAP

from helper.project_helper import get_project_path

# Create an Apprise instance

apprise_asset = AppriseAsset(app_id="BeecostVN", app_desc="Beecost Mua thông minh", app_url="https://beecost.vn",
                             image_url_mask="https://lh3.googleusercontent.com/pw/ACtC-3dfRz_FIcbjytzGLSxmuw7-CaBlF1R2HW-nkDr_MKl4KgBgNL-oniRADilIPEN7qTuUaui3fcKIXzxnsCXoiWXcPFfvz1stf5P6E8N6zTBDWEOjoNAiH3PIFS30KkrmqCTuW4UpF5MsNZ9vR67Fs8YS=w180")

DEVICE_ID = "cFPrBPSKwdl4WH_-NC7alw:APA91bHxORJu4NTz0P7juYTT6SkpS7kWDUkHo3mARCg15OSGEEmPlc96pNKTOEkg8zBXp1Cg7dQ7Z-eng2XGtc2dzeUVUVph-JDH_sD_jgg7CYMDFpxxNMduzQKmGX0SiNkBO5cDguS9"
PROJECT = "bee-indexing"
KEYFILE = os.path.join(get_project_path(), "constant/gcloud/key/bee-indexing-firebase-cloud-message.json")
FCM_URL = f'fcm://{PROJECT}/{DEVICE_ID}/?keyfile={KEYFILE}'
SCHEMA_MAP["fcm"].enabled = True
apobj = Apprise(debug=True)

apobj.add(FCM_URL)

apobj.notify(
    title='Beecost Price Tracking',
    body='Đang giảm giá nè =))'
)
