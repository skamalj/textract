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

start = time.time()
pdf = pdfium.PdfDocument("./togaf.pdf")
version = pdf.get_version()
n_pages = 5
num_workers = 1
result = []
session = get_session()

config = Config(
   retries = {
      'max_attempts': 1,
      'mode': 'standard'
   },
   region_name='us-west-2'
)


async def get_text_s3(image, index):
    start = time.time()
    print(os.getpid(),"Image",index, "send to textract at",str(datetime.now()))
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    async with session.create_client('textract', config=config) as client:
        response = await client.detect_document_text(
                Document={'S3Object':
                        {
                            'Bucket':"skamalj-wus",
                            'Name':"out_%0*d.jpg" % (3, index)
                        }
                })
    print(os.getpid(), "Image",index,"detected",len(response['Blocks']), "blocks in", time.time() - start,
       "seconds","at",response['ResponseMetadata']['HTTPHeaders']['date'],
       "with",response['ResponseMetadata']['RetryAttempts'],"retries")
    #print(response)
    return len(response['Blocks'])

async def get_text(image, index):
    start = time.time()
    print(os.getpid(),"Image",index, "send to textract at",str(datetime.now()))
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    async with session.create_client('textract', config=config) as client:
        response = await client.detect_document_text(
                Document={'Bytes': buffer.getbuffer().tobytes()})
    print(os.getpid(), "Image",index,"detected",len(response['Blocks']), "blocks in", time.time() - start,
       "seconds","at",response['ResponseMetadata']['HTTPHeaders']['date'],
       "with",response['ResponseMetadata']['RetryAttempts'],"retries")
    #print(response)
    return len(response['Blocks'])
    


async def render_page(pdf, index):
    print(os.getpid(),"Render page at index",index)
    renderer = pdf.render_to(
        pdfium.BitmapConv.pil_image,
        page_indices = [index],
        scale = 250/72,  # 300dpi resolution
        )
    return next(renderer)

async def save_image_s3(image,index):
    start = time.time()
    print(os.getpid(),"Saving image to S3", index)
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    buffer.seek(0)
    async with session.create_client('s3', config=config) as client:
        await client.put_object(Bucket="skamalj-wus",Body=buffer.read(), Key="out_%0*d.jpg" % (3, index))
    print(os.getpid(),"Image Saved to S3", index, "in",time.time() - start, "seconds")
    
async def save_image(image,index):
    start = time.time()
    print(os.getpid(),"Saving image", index)
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    async with aiofiles.open("out_%0*d.jpg" % (3, index), "wb") as file:
        await file.write(buffer.getbuffer())
    print(os.getpid(),"Image Saved", index, "in",time.time() - start, "seconds")

# Model function ()
async def process_page(index):
    try:
        print(os.getpid(),"Processing page at index",index)
        start = time.time()
        image = await render_page(pdf, index)
        print(os.getpid(),"Page", index, "rendered in",time.time() - start, "seconds")
        #await save_image_s3(image,index)
        r = await get_text(image, index)
        await save_image(image,index)
        # Add your model function here
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
    loop.run_until_complete(get_results(tasks,start))