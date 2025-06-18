from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from mbbankchecker import MBBank
import pandas as pd
import unicodedata
import re
import os
import uuid
import shutil
from pathlib import Path
from dotenv import load_dotenv
import asyncio
import logging

load_dotenv()
app = FastAPI()
lock = asyncio.Lock()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

origins = [
    "http://localhost:3000",
    "https://bankchecker.netlify.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def remove_accents(text):
    if pd.isna(text):
        return ""
    text = str(text)
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    text = re.sub(r'\s+', ' ', text.lower().strip())
    return text

def compare_names(name1, name2):
    def normalize(name):
        if pd.isna(name):
            return ""
        name = str(name)
        name = name.replace("Đ", "D").replace("đ", "d")
        name = unicodedata.normalize('NFD', name)
        name = ''.join(c for c in name if unicodedata.category(c) != 'Mn')
        name = name.lower().strip()
        name = re.sub(r'[^a-z0-9]', ' ', name)   # bỏ dấu, ký tự đặc biệt, giữ chữ và số
        name = re.sub(r'\s+', ' ', name)         # chuẩn hóa khoảng trắng
        return name

    return normalize(name1) == normalize(name2)

def clean_account_number(account_number):
    if pd.isna(account_number):
        return ""
    account_str = str(account_number)
    if account_str.endswith('.0'):
        account_str = account_str[:-2]
    # Remove non-digits but preserve the original string format
    account_str = re.sub(r'[^\d]', '', account_str)
    return account_str.strip()


bank_map = {
  "314": "970441",
  "203": "970436",
  "204": "970405",
  "201": "970415",
  "202": "970418",
  "311": "970422",
  "310": "970407",
  "309": "970432",
  "303": "970403",
  "358": "970423",
  "302": "970426",
  "334": "970429",
  "333": "970448",
  "348": "970443",
  "304": "970406",
  "321": "970437",
  "317": "970440",
  "357": "970449",
  "616": "970424",
  "305": "970431",
  "323": "970425",
  "617": "458761",
  "313": "970409",
  "308": "970400",
  "355": "970427",
  "307": "970416",
  "359": "970438",
  "356": "970433",
  "341": "970430",
  "320": "970408",
  "602": "",
  "353": "970452",
  "306": "970428",
  "352": "970419",
  "360": "970412",
  "327": "970454",
  "339": "970444",
  "654": "",
  "601": "",
  "606": "",
  "611": "",
  "615": "",
  "622": "",
  "623": "",
  "625": "",
  "629": "",
  "630": "",
  "635": "",
  "642": "",
  "650": "796500",
  "651": "",
  "652": "970455",
  "653": "",
  "661": "422589",
  "664": "",
  "207": "999888",
  "208": "",
  "619": "",
  "604": "970410",
  "603": "970442",
  "648": "555666",
  "638": "555666",
  "639": "",
  "613": "",
  "636_1": "",
  "636_2": "",
  "501": "970439",
  "665": "970458",
  "620": "963688",
  "609": "",
  "649": "",
  "614": "963666",
  "657": "963668",
  "612": "",
  "663": "970457",
  "627": "",
  "632": "",
  "640": "",
  "641": "970456",
  "631": "970463",
  "666": "970462",
  "656": "970466",
  "626": "970467",
  "901": "970446",
  "502": "970434",
  "669": "668888",
  "605": "533948",
  "608": "",
  "DG01": "971100",
  "DG02": "971005",
  "DG03": "971011",
  "309A": "546034",
  "309B": "546035",
  "353A": "963399",
  "TM01": "963388",
  "DG04": "963369"
}

@app.get("/")
def home():
    return {"message": os.getenv("ACCOUNT")}

async def process_account(mb, row, index, results, retry_list=None):
    ma_ngan_hang = str(row['Ma_Ngan_Hang']).strip()
    so_tai_khoan = clean_account_number(row.iloc[2])
    ten_nguoi_nhan = str(row.iloc[1]).strip()
    row_number = index + 4

    if not ma_ngan_hang or ma_ngan_hang == 'nan' or not so_tai_khoan or so_tai_khoan == 'nan' or not ten_nguoi_nhan:
        results["invalid"].append({
            "account": {
                "accountNumber": so_tai_khoan or "N/A",
                "accountName": ten_nguoi_nhan or "N/A",
                "bankName": str(row.iloc[5]) or "N/A",
                "row": row_number
            },
            "errors": ["Missing required data (account number, name, or bank code)"]
        })
        return

    account_info = {
        "accountNumber": so_tai_khoan,
        "accountName": ten_nguoi_nhan,
        "bankName": str(row.iloc[5]),
        "row": row_number
    }

    if ma_ngan_hang in bank_map:
        bank_code = bank_map[ma_ngan_hang]
        transfer_type = "INHOUSE" if bank_code == "970422" else "FAST"

        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: mb.inquiryAccountName(
                    creditAccount=so_tai_khoan,
                    creditAccountType="ACCOUNT",
                    transfer_type=transfer_type,
                    bankCode=bank_code
                )
            )
            logger.info(f"API response for account {so_tai_khoan}: {result}")
            if 'benName' in result and result['benName']:
                is_name_match = compare_names(ten_nguoi_nhan, result['benName'])
                if is_name_match:
                    results["valid"].append(account_info)
                else:
                    results["invalid"].append({
                        "account": account_info,
                        "errors": [f"Account name mismatch (Bank: {result['benName']})"]
                    })
            else:
                results["invalid"].append({
                    "account": account_info,
                    "errors": ["No account name returned from bank"]
                })
        except Exception as e:
            logger.error(f"API error for account {so_tai_khoan}: {str(e)}")
            if "GW485" in str(e):
                if retry_list is not None:
                    retry_list.append((row, index))
            else:
                results["invalid"].append({
                    "account": account_info,
                    "errors": [f"Account verification failed: {str(e)}"]
                })
    else:
        results["invalid"].append({
            "account": account_info,
            "errors": [f"Unknown bank code: {ma_ngan_hang}"]
        })

@app.post("/check-file")
async def check_file(file: UploadFile = File(...)):
    async with lock:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(status_code=400, detail="Only Excel files (.xlsx, .xls) are allowed")

        temp_dir = Path("temp_files")
        temp_dir.mkdir(exist_ok=True)
        file_path = temp_dir / f"{uuid.uuid4()}_{file.filename}"

        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

            df = pd.read_excel(
                file_path, 
                skiprows=3,
                dtype={2: str}  # Assuming account number is in column 2 (0-indexed)
            )

            df_filtered = df[
                (df.iloc[:, 1].notna()) &
                (df.iloc[:, 1] != '') &
                (df.iloc[:, 2].notna()) &
                (df.iloc[:, 2] != '') &
                (df.iloc[:, 5].notna()) &
                (df.iloc[:, 5] != '')
            ].copy()

            # if len(df_filtered) > 50:
            #     raise HTTPException(status_code=400, detail="File exceeds maximum row limit of 50")

            df_filtered['Ma_Ngan_Hang'] = df_filtered.iloc[:, 5].astype(str).str.split('-').str[0].str.strip()

            results = {"valid": [], "invalid": []}
            mb = MBBank(username=os.getenv("ACCOUNT"), password=os.getenv("PASSWORD"))
            account_count = 0

            # Initial processing of accounts
            retry_list = []
            for index, row in df_filtered.iterrows():
                await process_account(mb, row, index, results, retry_list)
                account_count += 1
                if account_count % 5 == 0:
                    logger.info(f"Processed {account_count} accounts, waiting 25 seconds")
                    await asyncio.sleep(25)
                else:
                    logger.info(f"Processed account {account_count}, waiting 1 seconds")
                    await asyncio.sleep(1)

            # Retry loop until retry_list is empty
            retry_round = 0
            while retry_list:
                retry_round += 1
                logger.info(f"Starting retry round {retry_round} with {len(retry_list)} accounts")
                
                # Create a new retry_list for the current round
                current_retry_list = retry_list.copy()
                retry_list.clear()  # Clear retry_list for the next round
                
                # Log out and reinitialize MBBank session for retry
                logger.info("Logging out and reinitializing MBBank session for retry...")
                mb.logout()
                del mb
                await asyncio.sleep(5)
                mb = MBBank(username=os.getenv("ACCOUNT"), password=os.getenv("PASSWORD"))

                # Process accounts in current_retry_list
                retry_count = 0
                for row, index in current_retry_list:
                    await process_account(mb, row, index, results, retry_list)
                    retry_count += 1
                    await asyncio.sleep(2)

                logger.info(f"Completed retry round {retry_round} with {retry_count} accounts processed")

            # Final logout
            logger.info("Final logout of MBBank session")
            mb.logout()

            return JSONResponse(content=results)

        except Exception as e:
            logger.error(f"File processing error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

        finally:
            if file_path.exists():
                os.remove(file_path)
