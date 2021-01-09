import os


class Trigger:
    def __init__(self):
        self.r, self.w = os.pipe()

    def notify(self):
        os.write(self.w, b'1')

    def close(self):
        try:
            os.close(self.r)
        except Exception:  # pragma: no cover
            pass

        try:
            os.close(self.w)
        except Exception:  # pragma: no cover
            pass

    def read(self):
        return os.read(self.r, 8192)

    def fileno(self):
        return self.r
