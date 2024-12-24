import emoji

# Функция для нормализации эмодзи
def normalize_emoji(emotion: str) -> str:
    return emoji.demojize(emotion)


