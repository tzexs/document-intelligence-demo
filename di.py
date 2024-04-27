# Importing Libraries
import os
from io import BytesIO
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from azure.core.credentials import AzureKeyCredential
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import PyPDF2
import filesystem_clients as filesystem_clients

load_dotenv()

# Interaction with Containers
filesystem_client_raw = filesystem_clients._create_filesystem_client_raw()
filesystem_client_silver = filesystem_clients._create_filesystem_client_silver()
filesystem_client_gold = filesystem_clients._create_filesystem_client_gold()

# Document Intelligence Credentials and DI Client
di_api_key = os.getenv("DI_API_KEY")
di_endpoint = os.getenv("DI_ENDPOINT")

document_analysis_client = DocumentAnalysisClient(
    endpoint=di_endpoint, credential=AzureKeyCredential(di_api_key)
)

def split_pdf(file_name):
    pdf_file = filesystem_clients.download_file(file_name, filesystem_client_raw)
    file_stream = BytesIO(pdf_file)

    reader = PyPDF2.PdfReader(file_stream)
    num_pages = len(reader.pages)

    for page_num in range(num_pages):
        writer = PyPDF2.PdfWriter()
        writer.add_page(reader.pages[page_num])

        page_number_str = str(page_num + 1).zfill(4)
        output_pdf_path = f"{os.path.splitext(file_name)[0]}_page_{page_number_str}.pdf"
        output_pdf_stream = BytesIO()

        writer.write(output_pdf_stream)
        output_pdf_stream.seek(0)

        file_client = filesystem_client_silver.get_file_client(output_pdf_path)
        file_client.upload_data(output_pdf_stream.read(), overwrite=True)
        convert_pdf_to_txt(output_pdf_path)


def convert_pdf_to_txt(file_name):
    try:
        pdf = filesystem_clients.download_file(file_name, filesystem_client_silver)
        poller = document_analysis_client.begin_analyze_document(
            "prebuilt-layout", document=pdf,
        )
        result = poller.result()
        print(f'Document analysis completed successfully for {file_name}')

        text = ''
        for page in result.pages:
            for line in page.lines:
                text += line.content
                text += "\n"

        name_combined_txt = f"{os.path.splitext(file_name)[0]}.txt"
        file_client = filesystem_client_gold.get_file_client(name_combined_txt)

        with BytesIO(text.encode("utf-8")) as combined_txt_stream:
            file_client.upload_data(combined_txt_stream, overwrite=True)

    except ResourceNotFoundError:
        print(f"Blob not found: {file_name}")
    except HttpResponseError as e:
        print(f"Form Recognizer Error: {e}")
        print(f"Status Code: {e.status_code}")
        print(f"Error Details: {e.reason}")
        print(f"Additional Details: {e.response.text}")
    except Exception as e:
        print(f"Error: {e}")
