#!/usr/bin/env python3
"""
Enhanced Fitbit Pipeline Management Script
Comprehensive system management, monitoring, and maintenance utilities.
"""

import os
import sys
import json
import argparse
import subprocess
import requests
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FitbitPipelineManager:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.env_file = self.base_dir / '.env'
        self.docker_compose_file = self.base_dir / 'docker-compose.yml'
        self.load_environment()
    
    def load_environment(self):
        """Load environment variables from .env file"""
        if self.env_file.exists():
            with open(self.env_file) as f:
                for line in f:
                    if line.strip() and not line.startswith('#'):
                        key, value = line.strip().split('=', 1)
                        os.environ[key] = value
    
    def check_prerequisites(self) -> Dict[str, bool]:
        """Check if all prerequisites are met"""
        checks = {
            'docker': self._check_docker(),
            'docker_compose': self._check_docker_compose(),
            'env_file': self.env_file.exists(),
            'fitbit_credentials': self._check_fitbit_credentials(),
            'influxdb_token': bool(os.getenv('INFLUX_TOKEN')),
            'grafana_credentials': bool(os.getenv('GRAFANA_ADMIN_USER') and os.getenv('GRAFANA_ADMIN_PASSWORD'))
        }
        return checks
    
    def _check_docker(self) -> bool:
        """Check if Docker is installed and running"""
        try:
            result = subprocess.run(['docker', '--version'], capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False
    
    def _check_docker_compose(self) -> bool:
        """Check if Docker Compose is available"""
        try:
            result = subprocess.run(['docker', 'compose', 'version'], capture_output=True, text=True)
            return result.returncode == 0
        except FileNotFoundError:
            return False
    
    def _check_fitbit_credentials(self) -> bool:
        """Check if required Fitbit credentials are set"""
        required_vars = ['FITBIT_CLIENT_ID', 'FITBIT_CLIENT_SECRET', 'FITBIT_ACCESS_TOKEN', 'FITBIT_REFRESH_TOKEN']
        return all(os.getenv(var) for var in required_vars)
    
    def setup_system(self):
        """Initial system setup"""
        logger.info("Starting system setup...")
        
        # Check prerequisites
        checks = self.check_prerequisites()
        failed_checks = [check for check, passed in checks.items() if not passed]
        
        if failed_checks:
            logger.error(f"Failed prerequisite checks: {', '.join(failed_checks)}")
            self._print_setup_instructions(failed_checks)
            return False
        
        # Create necessary directories
        self._create_directories()
        
        # Initialize InfluxDB and Grafana
        self._deploy_services()
        
        # Wait for services to be ready
        self._wait_for_services()
        
        # Setup Grafana dashboards
        self._setup_grafana_dashboards()
        
        logger.info("System setup completed successfully!")
        return True
    
    def _create_directories(self):
        """Create necessary directories"""
        directories = [
            'logs', 'tokens', 'backups', 'grafana/dashboards', 
            'grafana/datasources', 'nginx/ssl', 'nginx/logs'
        ]
        
        for directory in directories:
            dir_path = self.base_dir / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {dir_path}")
    
    def _deploy_services(self):
        """Deploy services using Docker Compose"""
        logger.info("Deploying services...")
        result = subprocess.run(
            ['docker', 'compose', 'up', '-d'],
            cwd=self.base_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Failed to deploy services: {result.stderr}")
            raise Exception("Service deployment failed")
        
        logger.info("Services deployed successfully")
    
    def _wait_for_services(self, timeout=300):
        """Wait for services to be ready"""
        logger.info("Waiting for services to be ready...")
        
        services = {
            'InfluxDB': f"http://localhost:8086/ping",
            'Grafana': f"http://localhost:3000/api/health"
        }
        
        start_time = datetime.now()
        
        for service_name, health_url in services.items():
            while True:
                if (datetime.now() - start_time).seconds > timeout:
                    raise Exception(f"Timeout waiting for {service_name}")
                
                try:
                    response = requests.get(health_url, timeout=5)
                    if response.status_code == 200:
                        logger.info(f"{service_name} is ready")
                        break
                except requests.RequestException:
                    pass
                
                logger.info(f"Waiting for {service_name}...")
                time.sleep(10)
    
    def _setup_grafana_dashboards(self):
        """Setup Grafana dashboards"""
        logger.info("Setting up Grafana dashboards...")
        
        grafana_url = "http://localhost:3000"
        username = os.getenv('GRAFANA_ADMIN_USER', 'admin')
        password = os.getenv('GRAFANA_ADMIN_PASSWORD', 'admin')
        
        # Import dashboards
        dashboard_files = [
            'jarvis_medical_suite_2025-06-13.json',
            'health_monitoring.json',
            'pots_monitoring.json'
        ]
        
        for dashboard_file in dashboard_files:
            dashboard_path = self.base_dir / dashboard_file
            if dashboard_path.exists():
                self._import_dashboard(grafana_url, username, password, dashboard_path)
    
    def _import_dashboard(self, grafana_url: str, username: str, password: str, dashboard_path: Path):
        """Import a single dashboard into Grafana"""
        try:
            with open(dashboard_path) as f:
                dashboard_json = json.load(f)
            
            import_data = {
                "dashboard": dashboard_json,
                "overwrite": True
            }
            
            response = requests.post(
                f"{grafana_url}/api/dashboards/db",
                json=import_data,
                auth=(username, password),
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info(f"Dashboard imported: {dashboard_path.name}")
            else:
                logger.error(f"Failed to import dashboard {dashboard_path.name}: {response.text}")
        
        except Exception as e:
            logger.error(f"Error importing dashboard {dashboard_path.name}: {str(e)}")
    
    def status(self) -> Dict:
        """Get system status"""
        logger.info("Checking system status...")
        
        status = {
            'services': self._get_service_status(),
            'health_checks': self._run_health_checks(),
            'data_freshness': self._check_data_freshness(),
            'api_limits': self._check_api_limits(),
            'disk_space': self._check_disk_space()
        }
        
        return status
    
    def _get_service_status(self) -> Dict:
        """Get Docker service status"""
        try:
            result = subprocess.run(
                ['docker', 'compose', 'ps', '--format', 'json'],
                cwd=self.base_dir,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                services = {}
                for line in result.stdout.strip().split('\n'):
                    if line:
                        service_info = json.loads(line)
                        services[service_info['Service']] = {
                            'status': service_info['State'],
                            'health': service_info.get('Health', 'N/A')
                        }
                return services
            else:
                return {'error': result.stderr}
        
        except Exception as e:
            return {'error': str(e)}
    
    def _run_health_checks(self) -> Dict:
        """Run comprehensive health checks"""
        health_checks = {}
        
        # Check InfluxDB
        try:
            influx_url = os.getenv('INFLUX_URL', 'http://localhost:8086')
            response = requests.get(f"{influx_url}/ping", timeout=5)
            health_checks['influxdb'] = response.status_code == 200
        except:
            health_checks['influxdb'] = False
        
        # Check Grafana
        try:
            response = requests.get("http://localhost:3000/api/health", timeout=5)
            health_checks['grafana'] = response.status_code == 200
        except:
            health_checks['grafana'] = False
        
        # Check Fitbit API connectivity
        health_checks['fitbit_api'] = self._test_fitbit_connection()
        
        return health_checks
    
    def _test_fitbit_connection(self) -> bool:
        """Test Fitbit API connectivity"""
        try:
            access_token = os.getenv('FITBIT_ACCESS_TOKEN')
            if not access_token:
                return False
            
            headers = {'Authorization': f'Bearer {access_token}'}
            response = requests.get(
                'https://api.fitbit.com/1/user/-/profile.json',
                headers=headers,
                timeout=10
            )
            
            return response.status_code == 200
        except:
            return False
    
    def _check_data_freshness(self) -> Dict:
        """Check how fresh the data is in InfluxDB"""
        # This would query InfluxDB to check the timestamp of the latest data
        # Implementation depends on InfluxDB client setup
        return {'last_sync': 'implementation_needed'}
    
    def _check_api_limits(self) -> Dict:
        """Check Fitbit API rate limit status"""
        # This would track API usage to estimate remaining rate limit
        return {'estimated_remaining': 'implementation_needed'}
    
    def _check_disk_space(self) -> Dict:
        """Check available disk space"""
        import shutil
        total, used, free = shutil.disk_usage(self.base_dir)
        
        return {
            'total_gb': round(total / (1024**3), 2),
            'used_gb': round(used / (1024**3), 2),
            'free_gb': round(free / (1024**3), 2),
            'usage_percent': round((used / total) * 100, 2)
        }
    
    def backup_data(self, output_path: Optional[str] = None):
        """Create a backup of InfluxDB data"""
        logger.info("Starting data backup...")
        
        if not output_path:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = self.base_dir / 'backups' / f'fitbit_backup_{timestamp}'
        
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Backup InfluxDB data
        backup_cmd = [
            'docker', 'exec', 'influxdb',
            'influxd', 'backup', '-portable', 
            '-db', 'fitbit', '/tmp/influxdb_backup'
        ]
        
        result = subprocess.run(backup_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Copy backup from container
            copy_cmd = [
                'docker', 'cp', 'influxdb:/tmp/influxdb_backup',
                str(output_path / 'influxdb_backup')
            ]
            
            subprocess.run(copy_cmd)
            
            # Cleanup temporary backup in container
            cleanup_cmd = ['docker', 'exec', 'influxdb', 'rm', '-r', '/tmp/influxdb_backup']
            subprocess.run(cleanup_cmd)
            
            logger.info(f"Backup completed: {output_path}")
        else:
            logger.error(f"Backup failed: {result.stderr}")
    
    def restore_data(self, backup_path: str):
        """Restore data from backup"""
        logger.info(f"Restoring data from: {backup_path}")
        
        backup_path = Path(backup_path)
        if not backup_path.exists():
            logger.error(f"Backup path does not exist: {backup_path}")
            return
        
        # Copy backup to container
        copy_cmd = [
            'docker', 'cp', str(backup_path / 'influxdb_backup'),
            'influxdb:/tmp/influxdb_restore'
        ]
        
        result = subprocess.run(copy_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Restore backup
            restore_cmd = [
                'docker', 'exec', 'influxdb',
                'influxd', 'restore', '-portable',
                '-db', 'fitbit', '/tmp/influxdb_restore'
            ]
            
            result = subprocess.run(restore_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("Data restored successfully")
            else:
                logger.error(f"Restore failed: {result.stderr}")
            
            # Cleanup
            cleanup_cmd = ['docker', 'exec', 'influxdb', 'rm', '-r', '/tmp/influxdb_restore']
            subprocess.run(cleanup_cmd)
        else:
            logger.error(f"Failed to copy backup to container: {result.stderr}")
    
    def update_tokens(self, access_token: str, refresh_token: str):
        """Update Fitbit tokens"""
        logger.info("Updating Fitbit tokens...")
        
        # Update environment file
        if self.env_file.exists():
            with open(self.env_file, 'r') as f:
                lines = f.readlines()
            
            with open(self.env_file, 'w') as f:
                for line in lines:
                    if line.startswith('FITBIT_ACCESS_TOKEN='):
                        f.write(f'FITBIT_ACCESS_TOKEN={access_token}\n')
                    elif line.startswith('FITBIT_REFRESH_TOKEN='):
                        f.write(f'FITBIT_REFRESH_TOKEN={refresh_token}\n')
                    else:
                        f.write(line)
        
        # Update running containers
        subprocess.run([
            'docker', 'compose', 'restart', 'fitbit-collector'
        ], cwd=self.base_dir)
        
        logger.info("Tokens updated successfully")
    
    def logs(self, service: str = None, follow: bool = False, lines: int = 100):
        """View service logs"""
        cmd = ['docker', 'compose', 'logs']
        
        if follow:
            cmd.append('--follow')
        
        cmd.extend(['--tail', str(lines)])
        
        if service:
            cmd.append(service)
        
        subprocess.run(cmd, cwd=self.base_dir)
    
    def _print_setup_instructions(self, failed_checks: List[str]):
        """Print setup instructions for failed checks"""
        instructions = {
            'docker': "Install Docker: https://docs.docker.com/get-docker/",
            'docker_compose': "Docker Compose is included with Docker Desktop",
            'env_file': "Create .env file using the provided template",
            'fitbit_credentials': "Set up Fitbit API credentials in .env file",
            'influxdb_token': "Set INFLUX_TOKEN in .env file",
            'grafana_credentials': "Set Grafana admin credentials in .env file"
        }
        
        print("\n" + "="*50)
        print("SETUP INSTRUCTIONS")
        print("="*50)
        
        for check in failed_checks:
            if check in instructions:
                print(f"❌ {check}: {instructions[check]}")
        
        print("="*50 + "\n")

def main():
    parser = argparse.ArgumentParser(description='Fitbit Pipeline Management')
    parser.add_argument('command', choices=[
        'setup', 'status', 'start', 'stop', 'restart', 
        'backup', 'restore', 'logs', 'update-tokens'
    ])
    parser.add_argument('--service', help='Specific service name')
    parser.add_argument('--follow', action='store_true', help='Follow logs')
    parser.add_argument('--lines', type=int, default=100, help='Number of log lines')
    parser.add_argument('--backup-path', help='Backup/restore path')
    parser.add_argument('--access-token', help='New Fitbit access token')
    parser.add_argument('--refresh-token', help='New Fitbit refresh token')
    
    args = parser.parse_args()
    
    manager = FitbitPipelineManager()
    
    if args.command == 'setup':
        manager.setup_system()
    
    elif args.command == 'status':
        status = manager.status()
        print(json.dumps(status, indent=2))
    
    elif args.command == 'start':
        subprocess.run(['docker', 'compose', 'up', '-d'], cwd=manager.base_dir)
    
    elif args.command == 'stop':
        subprocess.run(['docker', 'compose', 'down'], cwd=manager.base_dir)
    
    elif args.command == 'restart':
        if args.service:
            subprocess.run(['docker', 'compose', 'restart', args.service], cwd=manager.base_dir)
        else:
            subprocess.run(['docker', 'compose', 'restart'], cwd=manager.base_dir)
    
    elif args.command == 'backup':
        manager.backup_data(args.backup_path)
    
    elif args.command == 'restore':
        if not args.backup_path:
            print("Error: --backup-path is required for restore")
            sys.exit(1)
        manager.restore_data(args.backup_path)
    
    elif args.command == 'logs':
        manager.logs(args.service, args.follow, args.lines)
    
    elif args.command == 'update-tokens':
        if not args.access_token or not args.refresh_token:
            print("Error: Both --access-token and --refresh-token are required")
            sys.exit(1)
        manager.update_tokens(args.access_token, args.refresh_token)

if __name__ == '__main__':
    main()