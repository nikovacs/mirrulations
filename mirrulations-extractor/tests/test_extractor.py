from mirrextractor.extractor import Extractor
import pikepdf
from mirrmock.mock_rabbitmq import MockRabbit


def mock_pdf_extraction(mocker):
    mocker.patch('pikepdf.open', return_value=pikepdf.Pdf.new())
    mocker.patch('pikepdf.Pdf.save', return_value=None)
    mocker.patch('pdfminer.high_level.extract_text', return_value='test')
    mocker.patch('os.makedirs', return_value=None)
    mocker.patch("builtins.open", mocker.mock_open())


def mock_text_extraction(mocker):
    mocker.patch.object(
        Extractor,
        'extract_text',
        return_value=None
    )


def test_extract_text(capfd, mocker):
    mock_pdf_extraction(mocker)
    Extractor.extract_text('a.pdf', 'b.txt')
    assert "Extracting text from a.pdf" in capfd.readouterr()[0]


def test_extract_text_non_pdf(capfd, mocker):
    mock_pdf_extraction(mocker)
    Extractor.extract_text('a.docx', 'b.txt')
    assert "FAILURE: attachment doesn't have appropriate extension a.docx" \
        in capfd.readouterr()[0]


def test_open_pdf_throws_pikepdf_error(mocker, capfd):
    mocker.patch('pikepdf.open', side_effect=pikepdf.PdfError)
    Extractor.extract_text('a.pdf', 'b.txt')
    assert "FAILURE: failed to open" in capfd.readouterr()[0]


def test_save_pdf_throws_runtime_error(mocker, capfd):
    mocker.patch('pikepdf.open', return_value=pikepdf.Pdf.new())
    mocker.patch('pikepdf.Pdf.save', side_effect=RuntimeError)
    Extractor.extract_text('a.pdf', 'b.txt')
    assert "FAILURE: failed to save" in capfd.readouterr()[0]


def test_text_extraction_throws_error(mocker, capfd):
    mocker.patch('pikepdf.open', return_value=pikepdf.Pdf.new())
    mocker.patch('pikepdf.Pdf.save', return_value=None)
    mocker.patch('pdfminer.high_level.extract_text', side_effect=ValueError)
    Extractor.extract_text('a.pdf', 'b.txt')
    assert "FAILURE: failed to extract text from" in capfd.readouterr()[0]


def test_extract_pdf(mocker, capfd):
    mocker.patch('pikepdf.open', return_value=pikepdf.Pdf.new())
    mocker.patch('pikepdf.Pdf.save', return_value=None)
    mocker.patch('pdfminer.high_level.extract_text', return_value='test')
    mocker.patch('os.makedirs', return_value=None)
    mocker.patch("builtins.open", mocker.mock_open())
    Extractor.extract_text('a.pdf', 'b.txt')
    assert "SUCCESS: Saved extraction at" in capfd.readouterr()[0]


def test_extract_queue_add_get_job():
    extractor = Extractor()
    extractor.extract_queue = MockRabbit()
    extractor.extract_queue.add('test')
    assert extractor.extraction_queue_size == 1
    assert extractor.extract_queue.get() == 'test'


def test_extract_next_in_queue(mocker, capfd):
    extractor = Extractor()
    extractor.extract_queue = MockRabbit()
    extractor.extract_queue.add('a.pdf')
    mocker.patch('mirrcore.path_generator.'
                 'PathGenerator.make_attachment_save_path',
                 return_value="b.pdf")
    mock_pdf_extraction(mocker)
    extractor.extract_next_in_queue()
    assert "SUCCESS: Saved extraction at" in capfd.readouterr()[0]


def test_extract_next_in_queue_queue_is_empty_nothing_happens(mocker, capfd):
    extractor = Extractor()
    extractor.extract_queue = MockRabbit()
    mock_pdf_extraction(mocker)
    extractor.extract_next_in_queue()
    assert capfd.readouterr()[0] == ''


def test_extract_next_in_queue_job_is_none_nothing_happens(mocker, capfd):
    extractor = Extractor()
    extractor.extract_queue = MockRabbit()
    extractor.extract_queue.add(None)
    mock_pdf_extraction(mocker)
    extractor.extract_next_in_queue()
    assert capfd.readouterr()[0] == ''
