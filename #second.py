#second
import cv2
import os, time
from PIL import Image, ImageFilter, ImageDraw, ImageFont
from queue import Queue
from threading import Thread

def extract_frames(video_path, output_folder):
    """
    Extracts frames from a video file.
    """
    os.makedirs(output_folder, exist_ok=True)

    cap = cv2.VideoCapture(video_path)
    count = 0

    if not cap.isOpened():
        print("ERROR: Cannot open video file")
        return

    while True:
        ret, frame = cap.read()
        if not ret:
            print("No more frames or failed to read.")
            break

        #print("Saving frame", count)

        success = cv2.imwrite(os.path.join(output_folder, f"frame_{count}.jpg"), frame)
        if not success:
            print(f"Failed to save frame {count}")
        count += 1

    cap.release()
    print(f"Extracted {count} frames")

def decode_image(path):
    return Image.open(path)

# SIMPLE filter (grayscale)
def apply_grayscale(img):
    return img.convert("L").convert("RGB")

# COMPLEX filter (blur)
def apply_blur(img):
    return img.filter(ImageFilter.GaussianBlur(radius=3))

# WATERMARK (semi-transparent)
def apply_watermark(img, text="fish !"):
    img = img.convert("RGBA")

    watermark = Image.new("RGBA", img.size)
    draw = ImageDraw.Draw(watermark)

    font = ImageFont.truetype("arial.ttf", 20)
    width, height = img.size

    for x in range(0, width, 200):
        for y in range(0, height, 50):
            draw.text((x, y), text, font=font, fill=(255, 255, 255, 80))

    combined = Image.alpha_composite(img, watermark)
    return combined.convert("RGB")

def save_image(img, path):
    img.save(path)

import re
def numeric_sort(files):
    return sorted(files, key=lambda x: int(re.findall(r"\d+", x)[0]))

def frames_to_video(input_folder, output_file, fps=24):
    images = numeric_sort(os.listdir(input_folder))

    # read first image to get size
    first_frame = cv2.imread(os.path.join(input_folder, images[0]))
    height, width, _ = first_frame.shape

    # video writer
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    video = cv2.VideoWriter(output_file, fourcc, fps, (width, height))

    for img_name in images:
        path = os.path.join(input_folder, img_name)
        frame = cv2.imread(path)

        video.write(frame)

    video.release()
    print("Video created:", output_file)


def play_video(path):
    cap = cv2.VideoCapture(path)

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        cv2.imshow("Video", frame)

        if cv2.waitKey(30) & 0xFF == ord('q'):
            break

    cap.release()
    cv2.destroyAllWindows()

def sequential_process(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    #print("Files found:", os.listdir(input_folder))

    for file in os.listdir(input_folder):
        path = os.path.join(input_folder, file)

        img = decode_image(path)
        img = apply_blur(img)          # or apply_grayscale
        img = apply_watermark(img)
        
        save_image(img, os.path.join(output_folder, file))

def pipeline_process(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    q1 = Queue()
    q2 = Queue()
    q3 = Queue()

    # Stage 1: Producer (read files)
    def load():
        for file in os.listdir(input_folder):
            q1.put(file)
        q1.put(None)

    # Stage 2: Decode
    def decode():
        while True:
            file = q1.get()
            if file is None:
                q2.put(None)
                break
            img = decode_image(os.path.join(input_folder, file))
            q2.put((file, img))

    # Stage 3: Filter
    def filter_stage():
        while True:
            item = q2.get()
            if item is None:
                q3.put(None)
                break
            file, img = item
            img = apply_blur(img)  # or grayscale
            q3.put((file, img))

    # Stage 4: Watermark + Save
    def save_stage():
        while True:
            item = q3.get()
            if item is None:
                break
            file, img = item
            img = apply_watermark(img)
            save_image(img, os.path.join(output_folder, file))

    # Run threads
    Thread(target=load).start()
    Thread(target=decode).start()
    Thread(target=filter_stage).start()
    Thread(target=save_stage).start()

from queue import Queue
from threading import Thread
import os

def producer_consumer_process(input_folder, output_folder):
    os.makedirs(output_folder, exist_ok=True)

    q = Queue()

    # Producer
    def producer():
        for file in os.listdir(input_folder):
            q.put(file)

        # stop signals
        for _ in range(4):
            q.put(None)

    # Consumer
    def consumer():
        while True:
            file = q.get()
            if file is None:
                break

            path = os.path.join(input_folder, file)

            img = decode_image(path)
            img = apply_blur(img)      # or grayscale
            img = apply_watermark(img)

            save_image(img, os.path.join(output_folder, file))

    # Start threads
    Thread(target=producer).start()

    for _ in range(4):  # 4 workers
        Thread(target=consumer).start()

if __name__ == "__main__":
    base_dir = r"C:\Users\user\Desktop\folder\frames1"
    output_dir = r"C:\Users\user\Desktop\folder\frames2"
    video_dir = r"C:\Users\user\Desktop\folder\video\MP4.mp4"
    result_dir = r"C:\Users\user\Desktop\folder\video\result.mp4"
    print("Video exists:", os.path.exists(video_dir))
    play_video(video_dir)
    start = time.time()
    extract_frames(video_dir, base_dir)
    sequential_process(base_dir, output_dir)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for sequential_process")
    frames_to_video(
    input_folder=output_dir,
    output_file=result_dir)
    play_video(result_dir)

    start = time.time()
    extract_frames(video_dir, base_dir)
    pipeline_process(base_dir, output_dir)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for pipeline_process")
    frames_to_video(
    input_folder=output_dir,
    output_file=result_dir)
    play_video(result_dir)

    start = time.time()
    extract_frames(video_dir, base_dir)
    producer_consumer_process(base_dir, output_dir)
    end = time.time()
    print(f"Time: {end - start:.4f} sec for producer_consumer_process")
    frames_to_video(
    input_folder=output_dir,
    output_file=result_dir)
    play_video(result_dir)
