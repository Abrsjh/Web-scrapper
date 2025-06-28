"""
User Agents Module

This module provides utilities for managing and rotating user agents.
"""

import logging
import random
from typing import List, Optional

# Logger
logger = logging.getLogger(__name__)

# List of common user agents
COMMON_USER_AGENTS = [
    # Chrome on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    
    # Chrome on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
    
    # Firefox on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0",
    
    # Firefox on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.2; rv:97.0) Gecko/20100101 Firefox/97.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.3; rv:98.0) Gecko/20100101 Firefox/98.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.3; rv:99.0) Gecko/20100101 Firefox/99.0",
    
    # Safari on macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15",
    
    # Edge on Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36 Edg/98.0.1108.56",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36 Edg/99.0.1150.39",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36 Edg/100.0.1185.50",
    
    # Mobile Chrome on Android
    "Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.101 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.58 Mobile Safari/537.36",
    
    # Mobile Safari on iOS
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_3_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
]


class UserAgentManager:
    """
    Manager for handling user agent rotation.
    
    This class provides functionality to rotate through a pool of user agents
    or generate random user agents for web requests.
    """
    
    def __init__(self, user_agents: Optional[List[str]] = None, random_rotation: bool = True):
        """
        Initialize the user agent manager.
        
        Args:
            user_agents: List of user agent strings to use. If None, default list is used.
            random_rotation: Whether to rotate user agents randomly or sequentially.
        """
        self.user_agents = user_agents or COMMON_USER_AGENTS.copy()
        self.random_rotation = random_rotation
        self.current_index = 0
        
        if not self.user_agents:
            logger.warning("No user agents provided. Using a default user agent.")
            self.user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36"]
    
    def get_next_user_agent(self) -> str:
        """
        Get the next user agent from the pool.
        
        Returns:
            Next user agent string based on rotation strategy
        """
        if self.random_rotation:
            return random.choice(self.user_agents)
        
        # Sequential rotation
        user_agent = self.user_agents[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.user_agents)
        
        return user_agent
    
    def get_random_user_agent(self) -> str:
        """
        Get a random user agent from the pool.
        
        Returns:
            Random user agent string
        """
        return random.choice(self.user_agents)
    
    def add_user_agent(self, user_agent: str) -> None:
        """
        Add a user agent to the pool.
        
        Args:
            user_agent: User agent string to add
        """
        if user_agent and user_agent not in self.user_agents:
            self.user_agents.append(user_agent)
    
    def remove_user_agent(self, user_agent: str) -> None:
        """
        Remove a user agent from the pool.
        
        Args:
            user_agent: User agent string to remove
        """
        if user_agent in self.user_agents and len(self.user_agents) > 1:
            self.user_agents.remove(user_agent)
    
    def clear_user_agents(self) -> None:
        """
        Clear all user agents from the pool.
        """
        self.user_agents = []
        self.current_index = 0
    
    def reset_user_agents(self) -> None:
        """
        Reset user agents to the default list.
        """
        self.user_agents = COMMON_USER_AGENTS.copy()
        self.current_index = 0


# Global instance for convenience
_user_agent_manager = UserAgentManager()


def get_random_user_agent() -> str:
    """
    Get a random user agent.
    
    Returns:
        Random user agent string
    """
    return _user_agent_manager.get_random_user_agent()


def get_next_user_agent() -> str:
    """
    Get the next user agent based on rotation strategy.
    
    Returns:
        Next user agent string
    """
    return _user_agent_manager.get_next_user_agent()


def add_user_agent(user_agent: str) -> None:
    """
    Add a user agent to the global pool.
    
    Args:
        user_agent: User agent string to add
    """
    _user_agent_manager.add_user_agent(user_agent)


def set_user_agents(user_agents: List[str]) -> None:
    """
    Set the list of user agents for the global pool.
    
    Args:
        user_agents: List of user agent strings
    """
    _user_agent_manager.clear_user_agents()
    for user_agent in user_agents:
        _user_agent_manager.add_user_agent(user_agent)


def reset_user_agents() -> None:
    """
    Reset user agents to the default list.
    """
    _user_agent_manager.reset_user_agents()