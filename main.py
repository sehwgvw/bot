# -*- coding: utf-8 -*-
import config
import database

import math
import logging
from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardRemove, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher.filters import CommandStart


# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher(bot)


@dp.message_handler(commands=['admin'])
async def send_welcome(message: types.Message):
    if message.chat.type == 'private':
        chat_id = message.from_user.id
        if chat_id in config.ADMINs_ID:
            users = len(database.getAllUserFromDatabase())
            await bot.send_sticker(chat_id=chat_id, sticker='CAACAgIAAxkBAAEL0OhmCB56BOX4noR4Z4j89f836SgvJQACN3cAAp7OCwABr0i5HcgVLJw0BA')
            keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
            keyboard.row('📩 Рассылка', '🗃 Список файлов')
            keyboard.row('🚪 Выйти из админки')
            await bot.send_message(chat_id, f"👋 Добро пожаловать в админку!\n👥 Пользователей в боте: {users}", parse_mode='html', reply_markup=keyboard)


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    if message.chat.type == 'private':
        chat_id = message.from_user.id
        args = message.get_args()
        database.newUserRegister(chat_id, message.date)
        if not args:
            await bot.send_sticker(chat_id=chat_id, sticker='CAACAgIAAxkBAAEL0DBmBw9wu-dHsw0-d3l6dSqZIYzh7wACEncAAp7OCwABLt3915EIVS40BA')
            await bot.send_message(chat_id, """
👋 Добро пожаловать!

🤖 Я бот-файлообменник, который поможет тебе делиться файлами без рекламы и ожидания!
☁️ Так-же ты можешь зайти в свой профиль, добавить/удалить или получить ссылки на загруженные тобой файлы!
            """, parse_mode='html', reply_markup=AllKeyboards('main_menu', chat_id))
        
        else:
            file_data = database.getFileData(args)
            if file_data:
                await bot.send_sticker(chat_id=chat_id, sticker='CAACAgIAAxkBAAEL0FFmByNbbkL7cBGzUbwH3E30zpS3yAACMHcAAp7OCwABKaqxZ3mxOyw0BA')
                file_id = file_data[0]
                file_full_id = file_data[1]
                file_name = file_data[2]
                file_size = file_data[3]
                file_downloads = int(file_data[4])+1
                file_user_uploader_id = file_data[5]

                database.setFileDownloads(file_id)
                await bot.send_document(chat_id=chat_id, document=file_full_id, caption=f"""
<b>💾 Имя файла:</b> <i>{file_name}</i>
<b>🎈 Размер файла:</b> <i>{file_size} кб</i>
<b>👁 Просмотрен:</b> <i>{file_downloads} раз</i>

<b>👨🏻‍💻 Файл загружен пользователем:</b> <i>{file_user_uploader_id}</i>
<b>📎 Ссылка на файл:</b> <code>t.me/{config.BOT_LOGIN}?start={file_id}</code>
                    """, parse_mode='html', reply_markup=AllKeyboards('main_menu', chat_id))

            else:
                await bot.send_sticker(chat_id=chat_id, sticker='CAACAgIAAxkBAAEL0FdmByeXHgG7702LgO4AAd15JKMXT-oAAiN3AAKezgsAAXvV5IAzLNuCNAQ')
                await bot.send_message(chat_id, f"❌ <b>Кажется файл был удален!</b>", parse_mode='html')


@dp.message_handler(content_types=['any'])
async def send_file(message: types.Message):
    if database.getUserFromDatabase(message.from_user.id) == False:
        await bot.send_message(message.from_user.id, f"❌ Требуется перезапуск бота, введите /start", parse_mode='html')
    else:
        if message.chat.type == 'private':
            chat_id = message.from_user.id

            if database.getUserMenu(chat_id) == "рассылка":
                users = database.getAllUserFromDatabase()
                for user in users:
                    await message.copy_to(user[0])
                    database.setUserMenu(chat_id, "menu")
                return
            else:
                database.setUserMenu(chat_id, "menu")

            if message.document:
                chat_id = message.from_user.id
                file_name = message.document.file_name
                file_size = round((int(message.document.file_size)/1024), 2)
                file_id = message.document.file_id
                file_unique_id = message.document.file_unique_id

                added = database.addFileData(file_unique_id, file_id, file_name, file_size, chat_id)
                if added == True:
                    await bot.send_sticker(chat_id=chat_id, sticker='CAACAgIAAxkBAAEL0DRmBw93DTbQARyWQCox3oTGfb_vUQACLncAAp7OCwACyMkSdgrmczQE')
                    await bot.send_message(chat_id, f"""
<b>✅ Файл успешно сохранен</b>

<b>💾 Имя файла:</b> <i>{file_name}</i>
<b>🎈 Размер файла:</b> <i>{file_size} кб</i> 
t.me/end_soft
<b>📎 Ссылка на файл:</b> <code>t.me/{config.BOT_LOGIN}?start={file_unique_id}</code>
                    """, parse_mode='html', reply_markup=AllKeyboards('main_menu', chat_id))
                else:
                    await bot.send_message(chat_id, f"❌ <b>Такой файл уже был добавлен!\n📎 Ссылка на файл:<b> <i>{added}</i>", parse_mode='html')


            if message.text:
                if message.text == '📩 Рассылка':
                    database.setUserMenu(chat_id, "рассылка")
                    await bot.send_message(chat_id, f"<b>📩 Введите сообщение для отправки всем пользователям бота!</b>", parse_mode='html')

                elif message.text == '🗃 Список файлов':
                    all_files = database.getAllFilesFromDatabase()
                    keyboard = types.InlineKeyboardMarkup()
                    button_1, button_2 = False, False
                    for file in all_files:
                        file_id = file[0]
                        file_name = file[2]

                        lines_change = 0
                        lines_max = math.ceil(len(all_files)/14)

                        if button_1 == False:
                            button_1 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                        elif button_2 == False:
                            button_2 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                            keyboard.row(button_1, button_2)
                            button_1, button_2 = False, False
                            lines_change += 2
                            if lines_change >= 16:
                                break

                    if button_1 != False:
                        keyboard.row(button_1)

                    back_page = types.InlineKeyboardButton(text="Назад", callback_data=f"fi_to_{lines_max}")
                    other = types.InlineKeyboardButton(text=f"1/{lines_max}", callback_data=f"other")
                    next_page = types.InlineKeyboardButton(text="Вперед", callback_data=f"fi_to_{2}")
                    keyboard.row(back_page,other,next_page)

                    await bot.send_message(chat_id, f"<b>💾 Выберите файл чтобы посмотреть его статистику или удалить!</b>", parse_mode='html', reply_markup=keyboard)

                elif message.text == 'ℹ️ FAQ':
                    await bot.send_message(chat_id, config.FAQ_TEXT, parse_mode='html')

                elif message.text == '🚪 Выйти из админки':
                    await bot.send_sticker(chat_id=chat_id, sticker='CAACAgIAAxkBAAEL0DBmBw9wu-dHsw0-d3l6dSqZIYzh7wACEncAAp7OCwABLt3915EIVS40BA')
                    await bot.send_message(chat_id, """
👋 Добро пожаловать!

🤖 Я бот-файлообменник, который поможет тебе делиться файлами без рекламы и ожидания!
☁️ Так-же ты можешь зайти в свой профиль, добавить/удалить или получить ссылки на загруженные тобой файлы!
                    """, parse_mode='html', reply_markup=AllKeyboards('main_menu', chat_id))

                elif message.text == '👤 Профиль':
                    user = database.getUserFromDatabase(chat_id)
                    user_file_list = database.getUserFilesList(chat_id)
                    all_downloads = 0
                    for file_id in user_file_list:
                        file = database.getFileData(file_id)
                        try:
                            all_downloads += int(file[4])
                        except Exception as e:
                            pass


                    keyboard = types.InlineKeyboardMarkup()
                    my_files = types.InlineKeyboardButton(text="💾 Мои файлы", callback_data="my_files")
                    keyboard.row(my_files)

                    await bot.send_sticker(chat_id=chat_id, sticker='CAACAgIAAxkBAAEL0FlmBzG6kKGv7spVKBDLaz75OVgFmQACIncAAp7OCwAB5UsJHaHUVwk0BA')
                    await bot.send_message(chat_id, f"""
<b>👨🏻‍💻 {message.from_user.first_name}</b>
<b>🕰 Дата регистрации:</b> <i>{user[1]}</i>

<b>💾 Загружено файлов:</b> <i>{len(user_file_list)} шт</i>
<b>👁 Просмотрено ваших файлов:</b> <i>{all_downloads} раз</i>
                        """, parse_mode='html', reply_markup=keyboard)





@dp.callback_query_handler(lambda call: True)
async def callback_inline(call):
    if database.getUserFromDatabase(call.message.chat.id) == False:
        await bot.send_message(call.message.chat.id, f"❌ Требуется перезапуск бота, введите /start", parse_mode='html')
    else:
        if call.message.chat.type == 'private':
            chat_id = call.message.chat.id
            if 'f_' in call.data:
                file_data = database.getFileData(call.data[2:])
                file_id = file_data[0]
                file_full_id = file_data[1]
                file_name = file_data[2]
                file_size = file_data[3]
                file_downloads = int(file_data[4])
                file_user_uploader_id = file_data[5]

                keyboard = types.InlineKeyboardMarkup()
                delete_file = types.InlineKeyboardButton(text="❌ Удалить этот файл", callback_data=f"del_{file_id}")
                my_files = types.InlineKeyboardButton(text="◀️ Назад к файлам", callback_data="my_files")
                keyboard.row(delete_file)
                keyboard.row(my_files)

                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"""
<b>💾 Имя файла:</b> <i>{file_name}</i>
<b>🎈 Размер файла:</b> <i>{file_size} кб</i>
<b>👁 Просмотрен:</b> <i>{file_downloads} раз</i>

<b>👨🏻‍💻 Файл загружен пользователем:</b> <i>{file_user_uploader_id}</i>
<b>📎 Ссылка на файл:</b> <code>t.me/{config.BOT_LOGIN}?start={file_id}</code>
                """, parse_mode='html', reply_markup=keyboard)


            elif 'del_' in call.data:
                file_id = call.data[4:]
                database.deleteFileFromDataAndUser(file_id)
                keyboard = types.InlineKeyboardMarkup()
                profile = types.InlineKeyboardButton(text="◀️ Назад в профиль", callback_data="profile")
                keyboard.row(profile)
                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"✅ Этот файл был удален!", parse_mode='html', reply_markup=keyboard)

            elif call.data == 'my_files':
                keyboard = types.InlineKeyboardMarkup()
                user_file_list = database.getUserFilesList(chat_id)

                if len(user_file_list) > 0:
                    lines_change = 0
                    lines_max = math.ceil(len(user_file_list)/14)
                    button_1, button_2 = False, False
                    for file_id in user_file_list:
                        try:
                            file_name = database.getFileData(file_id)[2]
                            if button_1 == False:
                                button_1 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                            elif button_2 == False:
                                button_2 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                                keyboard.row(button_1, button_2)
                                button_1, button_2 = False, False
                                lines_change += 2
                                if lines_change >= 16:
                                    break
                                
                        except Exception as e:
                            pass

                    if button_1 != False:
                        keyboard.row(button_1)


                    back_page = types.InlineKeyboardButton(text="Назад", callback_data=f"go_to_{lines_max}")
                    other = types.InlineKeyboardButton(text=f"1/{lines_max}", callback_data=f"other")
                    next_page = types.InlineKeyboardButton(text="Вперед", callback_data=f"go_to_{2}")
                    keyboard.row(back_page,other,next_page)

                    profile = types.InlineKeyboardButton(text="◀️ Назад в профиль", callback_data="profile")
                    keyboard.row(profile)

                    await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"<b>💾 Выберите файл чтобы посмотреть его статистику или удалить!</b>", parse_mode='html', reply_markup=keyboard)

                else:
                    await call.answer(text="☁️ У Вас нет загруженных файлов!", show_alert=True)

            elif 'go_to_' in call.data:
                page = int(call.data[6:])

                keyboard = types.InlineKeyboardMarkup()
                user_file_list = database.getUserFilesList(chat_id)

                lines_change = 0
                lines_max = math.ceil(len(user_file_list)/14)
                button_1, button_2 = False, False
                for file_id in user_file_list:
                    lines_change += 1
                    if lines_change >= page*14-14:
                        file_name = database.getFileData(file_id)[2]
                        if button_1 == False:
                            button_1 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                        elif button_2 == False:
                            button_2 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                            keyboard.row(button_1, button_2)
                            button_1, button_2 = False, False
                            if lines_change > page*14:
                                break

                if button_1 != False:
                    keyboard.row(button_1)

                if page > 1:
                    backp = page - 1
                    if page == lines_max:
                        nextp = 1
                    else:
                        nextp = page + 1
                else:
                    nextp = page + 1
                    backp = lines_max

                back_page = types.InlineKeyboardButton(text="Назад", callback_data=f"go_to_{backp}")
                other = types.InlineKeyboardButton(text=f"{page}/{lines_max}", callback_data=f"other")
                next_page = types.InlineKeyboardButton(text="Вперед", callback_data=f"go_to_{nextp}")
                keyboard.row(back_page,other,next_page)

                profile = types.InlineKeyboardButton(text="◀️ Назад в профиль", callback_data="profile")
                keyboard.row(profile)

                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"<b>💾 Выберите файл чтобы посмотреть его статистику или удалить!</b>", parse_mode='html', reply_markup=keyboard)


            elif 'fi_to_' in call.data:
                page = int(call.data[6:])

                keyboard = types.InlineKeyboardMarkup()
                user_file_list = database.getUserFilesList(chat_id)

                lines_change = 0
                lines_max = math.ceil(len(user_file_list)/14)
                button_1, button_2 = False, False
                for file_id in user_file_list:
                    lines_change += 1
                    if lines_change >= page*14-14:
                        file_name = database.getFileData(file_id)[2]
                        if button_1 == False:
                            button_1 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                        elif button_2 == False:
                            button_2 = types.InlineKeyboardButton(text=file_name, callback_data=f"f_{file_id}")
                            keyboard.row(button_1, button_2)
                            button_1, button_2 = False, False
                            if lines_change > page*14:
                                break

                if button_1 != False:
                    keyboard.row(button_1)

                if page > 1:
                    backp = page - 1
                    if page == lines_max:
                        nextp = 1
                    else:
                        nextp = page + 1
                else:
                    nextp = page + 1
                    backp = lines_max

                back_page = types.InlineKeyboardButton(text="Назад", callback_data=f"fi_to_{backp}")
                other = types.InlineKeyboardButton(text=f"{page}/{lines_max}", callback_data=f"other")
                next_page = types.InlineKeyboardButton(text="Вперед", callback_data=f"fi_to_{nextp}")
                keyboard.row(back_page,other,next_page)

                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"<b>💾 Выберите файл чтобы посмотреть его статистику или удалить!</b>", parse_mode='html', reply_markup=keyboard)
            elif call.data == 'profile':
                user = database.getUserFromDatabase(chat_id)
                user_file_list = database.getUserFilesList(chat_id)
                all_downloads = 0
                for file_id in user_file_list:
                    file = database.getFileData(file_id)
                    try:
                        all_downloads += int(file[4])
                    except Exception as e:
                        pass


                keyboard = types.InlineKeyboardMarkup()
                my_files = types.InlineKeyboardButton(text="💾 Мои файлы", callback_data="my_files")
                keyboard.row(my_files)

                await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=f"""
<b>👨🏻‍💻 {call.message.chat.first_name}</b>
<b>🕰 Дата регистрации:</b> <i>{user[1]}</i>

<b>💾 Загружено файлов:</b> <i>{len(user_file_list)} шт</i>
<b>👁 Просмотрено ваших файлов:</b> <i>{all_downloads} раз</i>
                    """, parse_mode='html', reply_markup=keyboard)


def AllKeyboards(value, chat_id):
    if value == 'main_menu':
        database.setUserMenu(chat_id, "main_menu")
        keyboard = ReplyKeyboardMarkup(resize_keyboard=True)
        keyboard.row('👤 Профиль', 'ℹ️ FAQ')
        return keyboard


print('>>> Бот запущен!')
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True) # skip_updates - пропускать накопившиеся сообщения
