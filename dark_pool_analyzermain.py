"""
Autonomous Dark Pool Analyzer - Main Orchestrator
Mission: Analyze and exploit inefficiencies in dark pools using advanced pattern recognition
Architecture: Modular system with Firebase state management and ML-powered detection
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime
from typing import Optional, Dict, Any

from .config import Config
from .firebase_client import FirebaseClient
from .data_ingestion import DataIngestionEngine
from .pattern_detector import PatternDetector
from .trading_signal import TradingSignalEngine
from .monitoring import SystemMonitor

logger = logging.getLogger(__name__)

class DarkPoolAnalyzer:
    """Main autonomous system orchestrator with graceful shutdown"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = Config(config_path)
        self.running = False
        self.components = {}
        
        # Setup graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    async def initialize(self) -> bool:
        """Initialize all system components with proper error handling"""
        try:
            logger.info("🚀 Initializing Dark Pool Analyzer System")
            
            # Initialize Firebase for state management
            self.components['firebase'] = FirebaseClient(self.config.firebase_config)
            await self.components['firebase'].initialize()
            
            # Initialize data ingestion pipeline
            self.components['data_ingestion'] = DataIngestionEngine(
                config=self.config.data_config,
                firebase_client=self.components['firebase']
            )
            
            # Initialize ML pattern detector
            self.components['pattern_detector'] = PatternDetector(
                model_config=self.config.model_config
            )
            
            # Initialize trading signal engine
            self.components['trading_signal'] = TradingSignalEngine(
                config=self.config.trading_config,
                firebase_client=self.components['firebase']
            )
            
            # Initialize system monitor
            self.components['monitor'] = SystemMonitor(
                firebase_client=self.components['firebase']
            )
            
            logger.info("✅ System initialization complete")
            return True
            
        except Exception as e:
            logger.error(f"❌ System initialization failed: {e}", exc_info=True)
            await self.shutdown()
            return False
    
    async def run(self) -> None:
        """Main execution loop with health monitoring"""
        if not await self.initialize():
            logger.error("Cannot start system due to initialization failure")
            return
        
        self.running = True
        logger.info("🎯 Starting main analysis loop")
        
        try:
            while self.running:
                # Check system health
                health_status = await self.components['monitor'].check_health()
                
                if not health_status['healthy']:
                    logger.warning(f"System health degraded: {health_status['issues']}")
                    await self._handle_health_issues(health_status)
                
                # Execute analysis cycle
                await self._analysis_cycle()
                
                # Report status to Firebase
                await self.components['monitor'].report_status({
                    'timestamp': datetime.utcnow().isoformat(),
                    'cycle_completed': True,
                    'health_status': health_status
                })
                
                # Controlled sleep to prevent CPU thrashing
                await asyncio.sleep(self.config.analysis_interval)
                
        except Exception as e:
            logger.error(f"💥 Critical system failure in main loop: {e}", exc_info=True)
            await self.shutdown()
    
    async def _analysis_cycle(self) -> Dict[str, Any]:
        """Execute one complete analysis cycle"""
        try:
            # 1. Ingest latest dark pool data
            raw_data = await self.components['data_ingestion'].fetch_latest_data()
            
            # 2. Process and validate data
            processed_data = await self.components['data_ingestion'].process_data(raw_data)
            
            # 3. Detect patterns using ML
            patterns = await self.components['pattern_detector'].analyze(processed_data)
            
            # 4. Generate trading signals
            signals = await self.components['trading_signal'].generate_signals(patterns)
            
            # 5. Store results for auditing
            await self._store_cycle_results({
                'timestamp': datetime.utcnow().isoformat(),
                'data_metrics': processed_data.get('metrics', {}),
                'patterns_detected': len(patterns),
                'signals_generated': len(signals)
            })
            
            return {
                'success': True,
                'patterns': patterns,
                'signals': signals
            }
            
        except Exception as e:
            logger.error(f"Analysis cycle failed: {e}")
            await self.components['monitor'].log_error('analysis_cycle', str(e))
            return {'success': False, 'error': str(e)}
    
    async def _store_cycle_results(self, results: Dict[str,