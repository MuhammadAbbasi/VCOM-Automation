import platform

# Monkeypatch platform.machine and platform.uname to bypass socket import deadlock on Python 3.14
platform.machine = lambda: 'AMD64'

class UnameResult:
    system = 'Windows'
    node = 'localhost'
    release = '10'
    version = '10.0.19045'
    machine = 'AMD64'
    processor = 'Intel64 Family 6 Model 158 Stepping 10, GenuineIntel'
    def __getitem__(self, item):
        return [self.system, self.node, self.release, self.version, self.machine, self.processor][item]

platform.uname = lambda: UnameResult()
