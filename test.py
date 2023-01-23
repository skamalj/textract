import asyncio
from time import sleep, time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import pypdfium2 as pdfium
import asyncio
import aiofiles
from io import BytesIO
from aiobotocore.session import get_session
from botocore.config import Config
import os


config = Config(
   retries = {
      'max_attempts': 1,
      'mode': 'standard'
   }
)

queue = asyncio.Queue()

pdf = pdfium.PdfDocument("./togaf.pdf")
n_pages = 3
result = []

async def get_text(image, index):
    start = time()
    print(os.getpid(), "Image",index, "send to textract")
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    session = get_session()
    async with session.create_client('textract', config=config) as client:
        response = await client.detect_document_text(
                Document={'Bytes': buffer.getbuffer().tobytes()})
    print(os.getpid(),"Image",index,"detected",len(response['Blocks']), "blocks in", time() - start,
        "seconds","at",response['ResponseMetadata']['HTTPHeaders']['date'],
        "with",response['ResponseMetadata']['RetryAttempts'],"retries")
    return len(response['Blocks'])

async def render_page(pdf, index):
    print(os.getpid(),"Render page",index)
    renderer = pdf.render_to(
        pdfium.BitmapConv.pil_image,
        page_indices = [index],
        scale = 300/72,  # 300dpi resolution
        )
    return next(renderer)

async def save_image(image,index):
    start = time()
    print(os.getpid(),"Saving image", index)
    buffer = BytesIO()
    image.save(buffer, format="JPEG")
    async with aiofiles.open("out_%0*d.jpg" % (3, index), "wb") as file:
        await file.write(buffer.getbuffer())
    print(os.getpid(),"Image Saved", index, "in",time() - start, "seconds")

async def save_page(index):
    image = await render_page(pdf, index)
    await save_image(image, index)
    return await get_text(image,index)
    

def work(i):
    loop = asyncio.get_event_loop()
    r = asyncio.run( save_page(i)) 
    return r

start = time()
async def producer(executor):
        loop = asyncio.get_event_loop()
        tasks = [loop.run_in_executor(executor, work,i) for i in range(n_pages)]
        for t in asyncio.as_completed(tasks):
            try:
                results = await t
                result.append(results)
            except e:
                print(os.getpid(),e)

if __name__ == "__main__":
    executor = ProcessPoolExecutor(max_workers=1)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(producer(executor)))
    print(os.getpid(),"Document parsed in",time() - start, "seconds")
    print(os.getpid(),len(result))
