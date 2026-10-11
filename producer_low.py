from model import MagnitudeBand
from producer import run_producer


if __name__ == "__main__":
    try:
        run_producer(MagnitudeBand.LOW)
    except KeyboardInterrupt:
        print("Low-magnitude producer stopped.")
