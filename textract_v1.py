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
from datetime import datetime

program_start = time.time()
pdf = pdfium.PdfDocument("./test2.pdf")
version = pdf.get_version()
n_pages = len(pdf)
num_workers = 16
result = []
session = get_session()
pages = []
config = Config(
   retries = {
      'max_attempts': 1,
      'mode': 'standard'
   },
   region_name='us-west-2'
)

async def masking():
    time.sleep(1)

async def get_text(buffer, index):
    start = time.time()
    start_time = str(datetime.now())
    print(os.getpid(),"Image",index, "send to textract at",start_time)
    async with session.create_client('textract', config=config) as client:
        response = await client.detect_document_text(
                Document={'Bytes': buffer.getbuffer().tobytes()})
    print(os.getpid(), "Image",index,"detected",len(response['Blocks']), "blocks in", time.time() - start,
       "seconds","at",response['ResponseMetadata']['HTTPHeaders']['date'],
       "with",response['ResponseMetadata']['RetryAttempts'],"retries","sent at",start_time)
    #print(response)
    return len(response['Blocks'])

async def render_page(pdf, index):
    print(os.getpid(),"Render page at index",index)
    start = time.time()
    renderer = pdf.render_to(
        pdfium.BitmapConv.pil_image,
        page_indices = [index],
        scale = 250/72,  # 300dpi resolution
        )
    image =  next(renderer)
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    print(os.getpid(),"Image buffer created for", index, "in",time.time() - start, "seconds")
    return buffer
    
async def save_image(buffer,index):
    print(os.getpid(),"Saving image",index)
    start = time.time()
    buffer.seek(0)
    async with aiofiles.open("out_%0*d.jpg" % (3, index), "wb") as file:
        await file.write(buffer.getbuffer())
    print(os.getpid(),"Image Saved", index, "in",time.time() - start, "seconds")

# Model function ()
async def process_page(index):
    try:
        image = await render_page(pdf, index)
        r = await get_text(image, index)
        await save_image(image,index)
        # Add your model function here
        await masking()
        return r
    except Exception as e:
        print(os.getpid(),"Error for image",index, e)
        return index*(-1)
        

def main(r):
    tasks = [asyncio.get_event_loop().create_task(process_page(i)) for i in r]
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
    return [t.result() for t in tasks]

async def get_results(tasks, start):
    for t in asyncio.as_completed(tasks):
        try:
            results = await t
            result.append(results)
        except Exception as e:
            print(os.getpid(),"Error when fetching result",e)
    print(result)
    print(os.getpid(),"Document Analysed in ", time.time() - start, "seconds")

if __name__ == "__main__":
    executor = ProcessPoolExecutor(max_workers=num_workers)
    loop = asyncio.get_event_loop()
    r = n_pages//num_workers
    tasks = [loop.run_in_executor(executor, main, [j for j in range(n_pages) if j%num_workers == i]) for i in range(num_workers)]
    loop.run_until_complete(get_results(tasks,program_start))
