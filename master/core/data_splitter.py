import os
import math
from typing import List
from utils.logger import get_logger


class DataSplitter:
    """
    Responsible for splitting large input files into smaller chunks (splits)
    that can be processed in parallel by Map workers.
    """

    def __init__(self):
        self.logger = get_logger(__name__)

    async def split_data(self, input_path: str, split_size_mb: int) -> List[str]:
        """
        Splits the input file into multiple smaller files (splits) of the specified size.

        Args:
            input_path (str): Path to the input file that needs to be split.
            split_size_mb (int): Target size of each split in megabytes.

        Returns:
            List[str]: A list containing the paths of the generated split files.
        """
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        file_size = os.path.getsize(input_path)
        split_size_bytes = split_size_mb * 1024 * 1024
        num_splits = math.ceil(file_size / split_size_bytes)

        splits = []

        # Read the entire content of the file into memory
        with open(input_path, 'r', encoding='utf-8') as input_file:
            content = input_file.read()

        # Divide content into approximately equal chunks
        chunk_size = len(content) // num_splits
        if chunk_size == 0:
            # Handle the edge case where file is smaller than requested split size
            chunk_size = len(content)
            num_splits = 1

        input_dir = os.path.dirname(input_path)
        base_name = os.path.splitext(os.path.basename(input_path))[0]

        for i in range(num_splits):
            start_idx = i * chunk_size
            # The last split takes the remainder of the content
            if i == num_splits - 1:
                end_idx = len(content)
            else:
                end_idx = (i + 1) * chunk_size

            chunk_content = content[start_idx:end_idx]
            split_path = os.path.join(input_dir, f"{base_name}_split_{i}")

            # Write the split to a physical file
            with open(split_path, 'w', encoding='utf-8') as split_file:
                split_file.write(chunk_content)

            splits.append(split_path)
            self.logger.debug(
                f"Created split {split_path} with {len(chunk_content)} characters"
            )

        self.logger.info(
            f"Data split into {num_splits} parts of ~{split_size_mb}MB each"
        )
        return splits

    def create_actual_splits(self, input_path: str, split_size_mb: int) -> List[str]:
        """
        Placeholder for a more advanced implementation of physical file splitting.

        Args:
            input_path (str): Path to the input file.
            split_size_mb (int): Target size of each split in megabytes.

        Returns:
            List[str]: A list of file paths for the generated splits (not implemented).
        """
        # TODO: Implement real file-based splitting mechanism.
        # For now, this is just a placeholder and returns nothing.
        pass