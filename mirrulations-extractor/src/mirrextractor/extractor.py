from datetime import datetime
import os
import time
import io
import pdfminer
import pdfminer.high_level
import pikepdf
from mirrcore.path_generator import PathGenerator
from mirrcore.rabbitmq import RabbitMQ


class Extractor:
    """
    Class containing methods to extract text from files.
    """
    def __init__(self):
        self.extract_queue = RabbitMQ('extraction_queue')

    @property
    def extraction_queue_size(self):
        """
        Returns the size of the extraction queue
        """
        return self.extract_queue.size()

    def extract_next_in_queue(self):
        """
        If there are any jobs in the extraction queue, this method will
        extract the text from the attachment and store it in the text
        """
        if self.extract_queue.size() <= 0:
            return
        attachment_path = self.extract_queue.get()
        if attachment_path is None:
            return
        self.extract_text(attachment_path, PathGenerator
                          .make_attachment_save_path(attachment_path))

    @staticmethod
    def extract_text(attachment_path, save_path):
        """
        This method takes a complete path to an attachment and determines
        which type of extraction will take place.
        *Note* save_path is for later use when saving the extracted text
        Parameters
        ----------
        attachment_path : str
            the complete file path for the attachment that is being extracted
            ex. /path/to/pdf/attachment_1.pdf
        save_path : str
            the complete path to store the extract text
            ex. /path/to/text/attachment_1.txt
        """
        # gets the type of the attachment file
        #   (ex. /path/to/pdf/attachment_1.pdf -> pdf)
        file_type = attachment_path[attachment_path.rfind('.')+1:]
        if file_type.endswith('pdf'):
            print(f"Extracting text from {attachment_path}")
            start_time = time.time()
            Extractor._extract_pdf(attachment_path, save_path)
            print(f"Time taken to extract text from {attachment_path}"
                  f" is {time.time() - start_time} seconds")
        else:
            print("FAILURE: attachment doesn't have appropriate extension",
                  attachment_path)

    @staticmethod
    def _extract_pdf(attachment_path, save_path):
        """
        This method takes a complete path to a pdf and stores
        the extracted text in the save_path.
        *Note* If a file exists at save_path, it will be overwritten.
        Parameters
        ----------
        attachment_path : str
            the complete file path for the attachment that is being extracted
            ex. /path/to/pdf/attachment_1.pdf
        save_path : str
            the complete path to store the extract text
            ex. /path/to/text/attachment_1.txt
        """
        try:
            pdf = pikepdf.open(attachment_path)
        except pikepdf.PdfError as err:
            print(f"FAILURE: failed to open {attachment_path}\n{err}")
            return
        pdf_bytes = io.BytesIO()
        try:
            pdf.save(pdf_bytes)
        except (RuntimeError, pikepdf.PdfError) as err:
            print(f"FAILURE: failed to save {attachment_path}\n{err}")
            return
        try:
            text = pdfminer.high_level.extract_text(pdf_bytes)
        except (ValueError, TypeError) as err:
            print("FAILURE: failed to extract "
                  f"text from {attachment_path}\n{err}")
            return
        # Make dirs if they do not already exist
        os.makedirs(save_path[:save_path.rfind('/')], exist_ok=True)
        # Save the extracted text to a file
        with open(save_path, "w", encoding="utf-8") as out_file:
            out_file.write(text.strip())
        print(f"SUCCESS: Saved extraction at {save_path}")


if __name__ == '__main__':
    # now = datetime.now()
    # while True:
    #     for (root, dirs, files) in os.walk('/data'):
    #         for file in files:
    #             # Checks for pdfs
    #             if not file.endswith('pdf'):
    #                 continue
    #             complete_path = os.path.join(root, file)
    #             output_path = PathGenerator\
    #                 .make_attachment_save_path(complete_path)
    #             if not os.path.isfile(output_path):
    #                 start_time = time.time()
    #                 Extractor.extract_text(complete_path, output_path)
    #                 print(f"Time taken to extract text from {complete_path}"
    #                       f" is {time.time() - start_time} seconds")
    #     # sleep for a hour
    #     current_time = now.strftime("%H:%M:%S")
    #     print(f"Sleeping for an hour : started at {current_time}")
    #     time.sleep(3600)
    extractor = Extractor()
    while True:
        if extractor.extraction_queue_size <= 0:
            print("Sleeping for an hour : started at "
                  f"{datetime.now().strftime('%H:%M:%S')}")
            time.sleep(3600)
            continue
        extractor.extract_next_in_queue()
