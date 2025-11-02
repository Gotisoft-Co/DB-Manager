
import sys
import pandas as pd
from urllib.parse import quote_plus
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String,
    DateTime, func, insert, select, update as sa_update, delete as sa_delete, text
)
from sqlalchemy.orm import sessionmaker
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QTextEdit, QFileDialog, QLabel
)
from PyQt6.QtCore import Qt

DB_REMOTE = {
    "HOST": "mysql.65e3ab49565f.hosting.myjino.ru",
    "PORT": 3306,
    "USER": "j30084097",
    "PASS": quote_plus("7f9vGAxSu"),
    "NAME": "j30084097"
}

DB_LOCAL = "sqlite:///local_emulator.db"

current_mode = "remote"  # remote или local
engine = None
SessionLocal = None
metadata = MetaData()
TEST_TABLE = "test_7"
CSV_PATH = "test_export.csv"

def init_engine():
    global engine, SessionLocal
    if current_mode == "remote":
        uri = (
            f"mysql+pymysql://{DB_REMOTE['USER']}:{DB_REMOTE['PASS']}"
            f"@{DB_REMOTE['HOST']}:{DB_REMOTE['PORT']}/{DB_REMOTE['NAME']}"
        )
    else:
        uri = DB_LOCAL

    engine = create_engine(uri, echo=False, future=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
    return engine

def get_session():
    return SessionLocal()


test_table = Table(
    TEST_TABLE,
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(200), nullable=False),
    Column("value", Integer, nullable=False, default=0),
    Column("created_at", DateTime, server_default=func.now())
)

def check_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Подключение успешно.")
        return True
    except Exception as e:
        print("Ошибка подключения:", e)
        return False

def create_table():
    test_table.create(bind=engine, checkfirst=True)
    print(f"Таблица {TEST_TABLE} создана.")

def drop_table():
    test_table.drop(bind=engine, checkfirst=True)
    print(f"Таблица {TEST_TABLE} удалена.")

def insert_row(name: str, value: int):
    with get_session() as session:
        session.execute(insert(test_table).values(name=name, value=value))
        session.commit()
    print(f"Добавлена строка: {name}, {value}")

def bulk_insert(rows):
    with get_session() as session:
        session.execute(insert(test_table), rows)
        session.commit()
    print(f"Вставлено строк: {len(rows)}")

def select_all(limit=None):
    with get_session() as session:
        stmt = select(test_table)
        if limit:
            stmt = stmt.limit(limit)
        res = session.execute(stmt).all()
        return [dict(r._mapping) for r in res]

def update_row(name: str, new_value: int):
    with get_session() as session:
        stmt = sa_update(test_table).where(test_table.c.name == name).values(value=new_value)
        res = session.execute(stmt)
        session.commit()
    print(f"Обновлено строк: {res.rowcount}")

def delete_row(name: str):
    with get_session() as session:
        stmt = sa_delete(test_table).where(test_table.c.name == name)
        res = session.execute(stmt)
        session.commit()
    print(f"Удалено строк: {res.rowcount}")

def export_to_csv(path: str):
    df = pd.DataFrame(select_all())
    df.to_csv(path, index=False)
    print(f"Экспортировано в {path}")

def import_from_csv(path: str):
    try:
        df = pd.read_csv(path, encoding="utf-8-sig").dropna(how="all")
        df.columns = [c.strip().lower() for c in df.columns]
        name_col = next((c for c in df.columns if "name" in c), None)
        value_col = next((c for c in df.columns if "value" in c or "val" in c), None)
        if not name_col or not value_col:
            print(f"Ошибка: в файле {path} нет подходящих колонок ('name' и 'value')")
            return
        df = df[[name_col, value_col]].dropna(subset=[name_col, value_col])
        df[name_col] = df[name_col].astype(str).str.strip()
        df[value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0).astype(int)
        df = df.rename(columns={name_col: "name", value_col: "value"})
        rows = df.to_dict(orient="records")
        if not rows:
            print("CSV не содержит валидных данных для вставки.")
            return
        bulk_insert(rows)
        print(f"Импортировано {len(rows)} строк из {path}")
    except Exception as e:
        print(f"Ошибка при импорте из CSV: {e}")

def run_all_tests():
    print(f"НАЧАЛО ТЕСТИРОВАНИЯ ({current_mode.upper()} режим)")
    try:
        drop_table()
    except Exception:
        pass

    create_table()
    insert_row("Alice", 10)
    bulk_insert([{"name": f"User{i}", "value": i} for i in range(1, 6)])
    print("Данные после вставки:")
    for r in select_all():
        print(r)

    update_row("User3", 333)
    delete_row("User1")
    export_to_csv(CSV_PATH)

    drop_table()
    create_table()
    import_from_csv(CSV_PATH)

    print("Финальные данные:")
    for r in select_all():
        print(r)
    print("ТЕСТ УСПЕШНО ЗАВЕРШЁН")


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DB Manager")
        self.resize(750, 550)

        layout = QVBoxLayout()
        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.mode_label = QLabel()
        self.mode_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        btns = {
            "Проверить подключение": self.check_conn,
            "Переключить режим": self.toggle_mode,
            "Создать таблицу": self.create_table,
            "Удалить таблицу": self.drop_table,
            "Добавить тестовую строку": self.insert_row,
            "Показать все строки": self.show_rows,
            "Экспорт в CSV": self.export_csv,
            "Импорт из CSV": self.import_csv,
            "Запустить автотест": self.run_tests
        }

        for text, func in btns.items():
            btn = QPushButton(text)
            btn.clicked.connect(func)
            layout.addWidget(btn)

        layout.addWidget(self.mode_label)
        layout.addWidget(self.log)
        self.setLayout(layout)
        self.update_mode_label()

    def append(self, msg):
        self.log.append(msg)

    def update_mode_label(self):
        self.mode_label.setText(f"Текущий режим: {current_mode.upper()}")

    def check_conn(self):
        ok = check_connection()
        self.append("Подключение: " + ("успешно" if ok else "ошибка"))

    def toggle_mode(self):
        global current_mode
        current_mode = "local" if current_mode == "remote" else "remote"
        init_engine()
        self.update_mode_label()
        self.append(f"Переключено на {current_mode.upper()} режим")

    def create_table(self):
        try:
            create_table()
            self.append("Таблица создана.")
        except Exception as e:
            self.append(f"Ошибка: {e}")

    def drop_table(self):
        try:
            drop_table()
            self.append("Таблица удалена.")
        except Exception as e:
            self.append(f"Ошибка: {e}")

    def insert_row(self):
        try:
            insert_row("GUI_User", 42)
            self.append("Добавлена строка GUI_User (42).")
        except Exception as e:
            self.append(f"Ошибка: {e}")

    def show_rows(self):
        try:
            rows = select_all()
            if not rows:
                self.append("Таблица пуста.")
            else:
                self.append("Содержимое таблицы:")
                for r in rows:
                    self.append(str(r))
        except Exception as e:
            self.append(f"Ошибка: {e}")

    def export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Сохранить CSV", "", "CSV Files (*.csv)")
        if path:
            try:
                export_to_csv(path)
                self.append(f"Экспортировано в {path}")
            except Exception as e:
                self.append(f"Ошибка: {e}")

    def import_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "Импорт CSV", "", "CSV Files (*.csv)")
        if path:
            try:
                import_from_csv(path)
                self.append(f"Импортировано из {path}")
            except Exception as e:
                self.append(f"Ошибка: {e}")

    def run_tests(self):
        try:
            run_all_tests()
            self.append("Автотест завершён (см. консоль).")
        except Exception as e:
            self.append(f"Ошибка: {e}")


if __name__ == "__main__":
    init_engine()

    if not check_connection():
        print("Не удалось подключиться к удалённой БД. Включён локальный режим.")
        current_mode = "local"
        init_engine()

    if len(sys.argv) > 1 and sys.argv[1] == "test":
        run_all_tests()
    else:
        app = QApplication(sys.argv)
        w = MainWindow()
        w.show()
        sys.exit(app.exec())
