import asyncio
import aiohttp
import datetime
# from aiohttp import ClientSession
from more_itertools import chunked
from models import engine, Session, Base, SwapiPeople


async def get_people(session, people_id: int):
    async with session.get(f"https://swapi.dev/api/people/{people_id}") as response:
        return await response.json() 
    

async def get_field(links: list):
    async with aiohttp.ClientSession() as session:
        if links:
            requests = [await session.get(link) for link in links]
            response = [await r.json(content_type=None) for r in requests]
            response = ", ".join([el.get('name') or el.get('title') for el in response])
            return response
        else:
            return None
        

async def paste_to_db(persons):
    if persons:
        swapi_person = [SwapiPeople(
            name=item.get('name'),
            birth_year=item.get('birth_year'),
            eye_color=item.get('eye_color'),
            films=await get_field(item.get('films')),
            gender=item.get('gender'),
            hair_color=item.get('hair_color'),
            height=item.get('height'),
            homeworld=item.get('homeworld'),
            mass=item.get('mass'),
            skin_color=item.get('skin_color'),
            species=await get_field(item.get('species')),
            starships=await get_field(item.get('starships')),
            vehicles=await get_field(item.get('vehicles'))) for item in persons]
    
    async with Session() as session:
        session.add_all(swapi_person)
        await session.commit()


async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.drop_all)
        await con.run_sync(Base.metadata.create_all)

    session = aiohttp.ClientSession()
    person_cor_list = (get_people(session, i) for i in range(1, 84))
    for chunk in chunked(person_cor_list, 5):
        persons = await asyncio.gather(*chunk)
        asyncio.create_task(paste_to_db(persons))

    await session.close()
    tasks = asyncio.all_tasks()
    for task in tasks:
        if task != asyncio.current_task():
            await task



if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.get_event_loop().run_until_complete(main())
    print(datetime.datetime.now() - start)
