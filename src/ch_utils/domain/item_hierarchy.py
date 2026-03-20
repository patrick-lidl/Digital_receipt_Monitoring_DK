from typing import List, Literal, Optional, TypeAlias

# Define the type alias
HierarchyLevel: TypeAlias = Literal["wgi_id", "uwg_id", "afam_id", "orig_item_nr"]


class ItemHierarchy:
    """Manages a fixed hierarchy of item levels and allows navigation through them."""

    hierarchy_levels: List[HierarchyLevel] = ["wgi_id", "uwg_id", "afam_id", "orig_item_nr"]

    def __init__(self, starting_index: int = 0) -> None:
        """
        Initializes the ItemHierarchy with a starting index.

        Args:
            starting_index (int): The index to start from in the hierarchy.

        Raises:
            ValueError: If the starting index is out of bounds.
        """
        if not (0 <= starting_index < len(self.hierarchy_levels)):
            raise ValueError("Invalid starting index.")
        self.current_value: HierarchyLevel = self.hierarchy_levels[starting_index]
        self.last_value: Optional[HierarchyLevel] = None

    def get_next(self) -> Optional[HierarchyLevel]:
        """
        Gets the next level in the hierarchy after the current level.

        Returns:
            Optional[HierarchyLevel]: The next level, or None if at the end of the hierarchy.
        """
        try:
            index: int = self.index_of(self.current_value)
            return self.hierarchy_levels[index + 1] if index + 1 < len(self.hierarchy_levels) else None
        except ValueError:
            return None

    def get_previous(self) -> Optional[HierarchyLevel]:
        """
        Gets the previous level in the hierarchy before the current level.

        Returns:
            Optional[HierarchyLevel]: The previous level, or None if at the beginning of the hierarchy.
        """
        try:
            index: int = self.index_of(self.current_value)
            return self.hierarchy_levels[index - 1] if index > 0 else None
        except ValueError:
            return None

    def move(self, value: HierarchyLevel) -> bool:
        """
        Moves the current level to the specified value in the hierarchy.

        Args:
            value (HierarchyLevel): The value to move to.

        Returns:
            bool: True if the move was successful, False if the value is not in the hierarchy.
        """
        if value in self.hierarchy_levels:
            self.last_value = self.current_value
            self.current_value = value
            return True
        return False

    def index_of(self, value: HierarchyLevel) -> int:
        """
        Gets the index of a given value in the hierarchy.

        Args:
            value (HierarchyLevel): The value to look up.

        Returns:
            int: The index of the value.

        Raises:
            ValueError: If the value is not found in the hierarchy.
        """
        return self.hierarchy_levels.index(value)

    def __repr__(self) -> str:
        """
        Returns a string representation of the current state of the hierarchy.

        Returns:
            str: A string showing the current and last levels.
        """
        return f"<ItemHierarchy current_value='{self.current_value}', last_value='{self.last_value}'>"
