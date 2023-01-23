import pypdfium2 as pdfium
import time
import asyncio
import aiofiles
from io import BytesIO
from aiobotocore.session import get_session
from botocore.config import Config
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
import os

pdf = pdfium.PdfDocument("./awssecurity.pdf")
version = pdf.get_version()  
n_pages = 16
num_workers = 4

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)

async def get_text(image, index):
    start = time.time()
    print(os.getpid(),"Image",index, "send to textract")
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    session = get_session()
    async with session.create_client('textract', config=config) as client:
        response = await client.detect_document_text(
                Document={'Bytes': buffer.getbuffer().tobytes()})
    print(os.getpid(), "Image",index,"detected",len(response['Blocks']), "blocks in", time.time() - start,
        "seconds","at",response['ResponseMetadata']['HTTPHeaders']['date'],
        "with",response['ResponseMetadata']['RetryAttempts'],"retries")
                
async def render_page(pdf, index):
    print(os.getpid(),"RRender page at index",index)
    renderer = pdf.render_to(
        pdfium.BitmapConv.pil_image,
        page_indices = [index],
        scale = 300/72,  # 300dpi resolution
        )
    return next(renderer)
    
async def save_image(image,index):
    start = time.time()
    print(os.getpid(),"Saving image", index)
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    async with aiofiles.open("out_%0*d.jpg" % (3, index), "wb") as file:
        await file.write(buffer.getbuffer())
    print(os.getpid(),"Image Saved", index, "in",time.time() - start, "seconds")

async def process_page(index):
    print(os.getpid(),"Processing page at index",index)
    image = await render_page(pdf, index)
    await get_text(image, index)
    await save_image(image,index)

def main(r):
    tasks = [asyncio.get_event_loop().create_task(process_page(i)) for i in r]
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks)) 

async def get_results(tasks, start):
    for t in asyncio.as_completed(tasks):
        try:
            results = await t
        except:
            print(os.getpid(),"Error")
    print(os.getpid(),"Document Analysed in ", time.time() - start, "seconds")

if __name__ == "__main__":
    start = time.time()    
    executor = ProcessPoolExecutor(max_workers=num_workers)
    loop = asyncio.get_event_loop()
    r = n_pages//num_workers
    tasks = [loop.run_in_executor(executor, main, range(i*r,min((i+1)*r,n_pages))) for i in range(num_workers)]
    loop.run_until_complete(get_results(tasks,start))
    