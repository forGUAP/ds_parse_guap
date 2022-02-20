import os
import discord
import logging
import pandas as pd
import datetime

logging.basicConfig(level=logging.INFO)

client = discord.Client()
guild = discord.Guild
log_channel_id = 000
staff_role_id = 000
bot_token = "your_tokn_here"
@client.event
async def on_ready():
    print('We have logged in as {0.user}'.format(client))
    await client.change_presence(activity=discord.Game('_scan help'))


@client.event
async def on_message(message):
    for r in message.author.roles:
        if r.id == staff_role_id:
            if message.author == client.user:
                return
            elif message.content.startswith('_'):
                cmd = message.content.split()[0].replace("_", "")
                parameters = message.content.split(", ")
                if cmd == 'scan':
                    data_join = pd.DataFrame(columns=['name', 'time_join'])  # Базы данных
                    data_left = pd.DataFrame(columns=['name', "time_left"])
                    data = pd.DataFrame(columns=['name', 'time_join', 'time_left', "time_spent"])
                    if len(message.channel_mentions) > 0:
                        channel = message.channel_mentions[0]
                    elif "help" in message.content:
                        answer = discord.Embed(title="Помощь",
                                               description="""`_scan <channel_id> <date time start> <date time end>`\n\n`<channel_id>` : **ID аудиканала**\n`<date time start>` : **ГГГГ-ММ-ДД ЧЧ:ММ - метка времени начала выгрузки**\n`<date time end>` : **ГГГГ-ММ-ДД ЧЧ:ММ - метка времени  выгрузки**""",
                                               colour=0x1a7794)
                        await message.channel.send(embed=answer)
                        return
                    else:
                        parameters[0] = parameters[0][parameters[0].find(' ') + 1:]
                        channel = await client.fetch_channel(int(parameters[0]))
                    log_channel = await client.fetch_channel(log_channel_id)
                    date_time_start = datetime.datetime.fromisoformat(parameters[1])-datetime.timedelta(hours=3)
                    date_time_end = datetime.datetime.fromisoformat(parameters[2])-datetime.timedelta(hours=3)
                    answer = discord.Embed(title="Создается отчетность",
                                           description="Пожалуйста подождите. Файл придёт вам в личные сообщения",
                                           colour=0x1a7794)

                    await message.channel.send(embed=answer)

                    def is_command(message):
                        if len(msg.content) == 0:
                            return False
                        elif msg.content.split()[0] == '_scan':
                            return True
                        else:
                            return False

                    async for msg in log_channel.history(limit=10000, before=date_time_end,
                                                         after=date_time_start):  #Парс до 10000 сообщений в заданом промежутке времени
                        if msg.author != client.user:  # пропустить команды
                            if not is_command(msg) and msg.embeds != []:
                                info = msg.embeds[0].to_dict()
                                description = info.get('description')
                                if str(channel.id) in description:
                                    userid = info.get('footer').get('text')
                                    userid = userid[userid.find(':') + 2:]
                                    user = await message.guild.fetch_member(userid)
                                    name = user.display_name
                                    if "joined" in description:
                                        time_join = (msg.created_at + datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S")
                                        data_join = data_join.append({'name': name, 'time_join': time_join}, ignore_index=True)
                                    if "left" in description:
                                        time_left = (msg.created_at + datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S")
                                        data_left = data_left.append({'name': name, 'time_left': time_left}, ignore_index=True)
                            if len(data_join) == 1000:
                                break
                    del_join, del_left = [], []
                    for student_join in data_join.itertuples():
                        for student_left in data_left.itertuples():
                            time_left = getattr(student_left, "time_left")
                            time_join = getattr(student_join, "time_join")
                            name = getattr(student_left, "name")
                            if time_left >= time_join and name == getattr(student_join, "name"):
                                time_spent = datetime.datetime.fromisoformat(time_left) - datetime.datetime.fromisoformat(time_join)
                                data = data.append({'name': name,
                                                    'time_join': time_join,
                                                    'time_left': time_left,
                                                    'time_spent': str(time_spent)}, ignore_index=True)
                                del_join.append(getattr(student_join, "Index"))
                                del_left.append(getattr(student_left, "Index"))
                                break
                    for d_j in del_join:
                        data_join.drop(labels=[d_j], axis=0, inplace=True)
                    for d_l in del_left:
                        data_left.drop(labels=[d_l], axis=0, inplace=True)
                    data = pd.concat([data, data_join, data_left])
                    file_location = f"{str(channel.guild.id) + '_' + str(channel.id)}.csv"
                    data.to_csv(file_location, index=False, sep="\t")
                    answer = discord.Embed(title="Вот ваш .CSV файл",
                                           description=f"""`Сервер` : **{message.guild.name}**\n`Канал` : **{channel.name}**""",
                                           colour=0x1a7794)
                    await message.author.send(embed=answer)
                    await message.author.send(file=discord.File(file_location, filename=f'{str(channel.name)}.csv')) # Отправляем файл
                    os.remove(file_location)  # Удаляем файл


client.run(bot_token)
