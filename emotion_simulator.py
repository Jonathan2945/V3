#!/usr/bin/env python3
"""
EMOTION SIMULATOR
================
Simulates trader emotions that affect trading decisions.
"""

import random
import logging
from typing import Dict, Tuple, List
from datetime import datetime

class EmotionSimulator:
    """Simulates trader emotions that affect decisions"""
    
    def __init__(self):
        self.fear_level = 0.5      # 0-1 scale
        self.greed_level = 0.5     # 0-1 scale
        self.confidence = 0.5      # 0-1 scale
        self.tilt_level = 0.0      # 0-1 scale (emotional tilt)
        
        # Track recent performance for emotional impact
        self.recent_trades = []
        self.consecutive_losses = 0
        self.consecutive_wins = 0
        
        logging.info("[EMO] Emotion Simulator initialized")
    
    def update_emotions(self, trade_result: Dict):
        """Update emotional state based on trade results"""
        if trade_result.get('pnl', 0) > 0:
            self.consecutive_wins += 1
            self.consecutive_losses = 0
            
            # Winning increases greed and confidence
            self.greed_level = min(1.0, self.greed_level + 0.1)
            self.confidence = min(1.0, self.confidence + 0.05)
            self.fear_level = max(0.0, self.fear_level - 0.05)
            
            # Too many wins can lead to overconfidence
            if self.consecutive_wins > 3:
                self.greed_level = min(1.0, self.greed_level + 0.15)
                
        else:
            self.consecutive_losses += 1
            self.consecutive_wins = 0
            
            # Losing increases fear and tilt
            self.fear_level = min(1.0, self.fear_level + 0.1)
            self.confidence = max(0.0, self.confidence - 0.1)
            self.tilt_level = min(1.0, self.tilt_level + 0.15)
            
            # Multiple losses cause emotional breakdown
            if self.consecutive_losses > 3:
                self.tilt_level = min(1.0, self.tilt_level + 0.25)
        
        # Store trade for history
        self.recent_trades.append(trade_result)
        if len(self.recent_trades) > 20:
            self.recent_trades.pop(0)
    
    def should_override_ml(self, ml_signal: Dict) -> Tuple[bool, str]:
        """Determine if emotions override ML decision"""
        override_probability = 0.0
        reason = ""
        
        # High fear might skip good trades
        if self.fear_level > 0.8 and ml_signal.get('signal_type') == 'BUY':
            override_probability += 0.3
            reason = "Too fearful to enter"
        
        # High greed might force bad trades
        if self.greed_level > 0.8:
            override_probability += 0.25
            reason = "Greed forcing trade"
        
        # Tilt causes random bad decisions
        if self.tilt_level > 0.7:
            override_probability += 0.4
            reason = "On tilt - emotional trading"
        
        # Low confidence might skip trades
        if self.confidence < 0.3:
            override_probability += 0.2
            reason = "Low confidence"
        
        # Actually override?
        if random.random() < override_probability:
            logging.warning(f"[EMOJI] EMOTION OVERRIDE: {reason}")
            return True, reason
        
        return False, ""
    
    def adjust_position_size(self, base_size: float) -> float:
        """Emotions affect position sizing"""
        multiplier = 1.0
        
        # Fear reduces position size
        multiplier *= (1.0 - (self.fear_level * 0.3))
        
        # Greed increases position size
        multiplier *= (1.0 + (self.greed_level * 0.4))
        
        # Tilt causes erratic sizing
        if self.tilt_level > 0.5:
            multiplier *= random.uniform(0.5, 2.0)
        
        # Confidence affects sizing
        multiplier *= (0.7 + (self.confidence * 0.6))
        
        return base_size * max(0.1, min(3.0, multiplier))
    
    def get_emotional_state(self) -> Dict:
        """Get current emotional state"""
        return {
            'fear_level': self.fear_level,
            'greed_level': self.greed_level,
            'confidence': self.confidence,
            'tilt_level': self.tilt_level,
            'consecutive_wins': self.consecutive_wins,
            'consecutive_losses': self.consecutive_losses,
            'recent_trades_count': len(self.recent_trades)
        }
    
    def reset_emotions(self):
        """Reset emotions to neutral state"""
        self.fear_level = 0.5
        self.greed_level = 0.5
        self.confidence = 0.5
        self.tilt_level = 0.0
        self.consecutive_losses = 0
        self.consecutive_wins = 0
        logging.info("[EMOJI] Emotions reset to neutral")
