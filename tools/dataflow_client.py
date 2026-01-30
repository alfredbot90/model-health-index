#!/usr/bin/env python3
"""
Dataflow Downloader for Microsoft Power BI
==========================================

Downloads dataflow definitions from Microsoft Power BI workspaces.
Supports downloading individual dataflows or all dataflows from a workspace.
"""

import os
import sys
import json
import requests
import base64
from datetime import datetime
from typing import Dict, List, Optional
import keyring
import subprocess


class DataflowDownloader:
    """
    Downloads dataflow definitions from Microsoft Power BI.
    """
    
    def __init__(self):
        self.base_url = "https://api.powerbi.com/v1.0/myorg"
        self.token = self._get_token()
        self.outputs_dir = "Outputs"
        
        # Create outputs directory if it doesn't exist
        if not os.path.exists(self.outputs_dir):
            os.makedirs(self.outputs_dir)
    
    def _get_token(self) -> str:
        """Get authentication token from environment or Azure CLI."""
        # Try environment variable first
        token = os.getenv('POWERBI_TOKEN')
        if token:
            return token
        
        # Try keyring
        try:
            token = keyring.get_password("powerbi", "token")
            if token:
                return token
        except:
            pass
        
        # Try Azure CLI
        try:
            import platform
            
            # Use shell=True on Windows to ensure az.cmd is found
            use_shell = platform.system() == "Windows"
            
            result = subprocess.run(
                ["az", "account", "get-access-token", "--resource", "https://analysis.windows.net/powerbi/api"],
                capture_output=True, 
                text=True, 
                check=True,
                shell=use_shell
            )
            token_data = json.loads(result.stdout)
            return token_data.get("accessToken", "")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ Error: No authentication token found.")
            print("💡 Please set POWERBI_TOKEN environment variable or run 'az login'")
            print("💡 You can also use: az account get-access-token --resource https://analysis.windows.net/powerbi/api")
            sys.exit(1)
    
    def make_request(self, method: str, url: str, **kwargs) -> Dict:
        """Make authenticated HTTP request to Power BI API."""
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
        
        if 'headers' in kwargs:
            headers.update(kwargs['headers'])
            del kwargs['headers']
        
        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            
            print(f"🔍 API Request: {method} {url}")
            print(f"📊 Status Code: {response.status_code}")
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"error": f"Not found: {url}"}
            else:
                print(f"❌ Response Text: {response.text}")
                return {"error": f"HTTP {response.status_code}: {response.text}"}
        except Exception as e:
            return {"error": f"Request failed: {str(e)}"}
    
    def get_workspace_dataflows(self, workspace_id: str) -> Dict:
        """Get all dataflows in a workspace using Power BI API."""
        url = f"{self.base_url}/groups/{workspace_id}/dataflows"
        return self.make_request('GET', url)
    
    def get_dataflow_details(self, workspace_id: str, dataflow_id: str) -> Dict:
        """Get details of a specific dataflow using Power BI API."""
        url = f"{self.base_url}/groups/{workspace_id}/dataflows/{dataflow_id}"
        return self.make_request('GET', url)
    
    def find_dataflow_by_name(self, workspace_id: str, dataflow_name: str) -> Optional[str]:
        """Find a dataflow by its display name and return its ID."""
        dataflows = self.get_workspace_dataflows(workspace_id)
        
        if 'error' in dataflows:
            print(f"❌ Error getting dataflows: {dataflows['error']}")
            return None
        
        for dataflow in dataflows.get('value', []):
            if dataflow.get('name') == dataflow_name:
                return dataflow.get('objectId')
        
        return None
    
    def download_single_dataflow(self, workspace_id: str, dataflow_name: str = None, dataflow_id: str = None) -> Dict:
        """Download a single dataflow definition."""
        if not dataflow_id and not dataflow_name:
            return {"error": "Must provide either dataflow_name or dataflow_id"}
        
        if not dataflow_id:
            dataflow_id = self.find_dataflow_by_name(workspace_id, dataflow_name)
            if not dataflow_id:
                return {"error": f"Dataflow '{dataflow_name}' not found in workspace"}
        
        # Get dataflow details
        details = self.get_dataflow_details(workspace_id, dataflow_id)
        if 'error' in details:
            return details
        
        # Get workspace name for filename
        workspace_name = self._get_workspace_name(workspace_id)
        
        # Create filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_workspace_name = "".join(c for c in workspace_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        safe_dataflow_name = "".join(c for c in details.get('name', 'Unknown') if c.isalnum() or c in (' ', '-', '_')).rstrip()
        
        filename = f"{safe_workspace_name}_{safe_dataflow_name}_{timestamp}_DATAFLOW.txt"
        filepath = os.path.join(self.outputs_dir, filename)
        
        # Write dataflow definition to file
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"Workspace Name: {workspace_name}\n")
                f.write(f"Workspace ID: {workspace_id}\n")
                f.write(f"Dataflow Name: {details.get('name', 'Unknown')}\n")
                f.write(f"Dataflow ID: {dataflow_id}\n")
                f.write(f"Description: {details.get('description', 'No description')}\n")
                f.write(f"Version: {details.get('version', 'Unknown')}\n")
                f.write(f"Culture: {details.get('culture', 'Unknown')}\n")
                f.write(f"Modified Time: {details.get('modifiedTime', 'Unknown')}\n")
                f.write("─" * 40 + "\n\n")
                
                # Write the full response for debugging
                f.write("FULL API RESPONSE:\n")
                f.write("=" * 50 + "\n")
                f.write(json.dumps(details, indent=2))
                
                # Extract queries metadata if available
                if 'pbi:mashup' in details and 'queriesMetadata' in details['pbi:mashup']:
                    f.write("\n\nQUERIES METADATA:\n")
                    f.write("=" * 50 + "\n")
                    queries = details['pbi:mashup']['queriesMetadata']
                    for query_name, query_info in queries.items():
                        f.write(f"\nQuery: {query_name}\n")
                        f.write(f"  Query ID: {query_info.get('queryId', 'Unknown')}\n")
                        f.write(f"  Query Name: {query_info.get('queryName', 'Unknown')}\n")
                        f.write(f"  Load Enabled: {query_info.get('loadEnabled', 'Unknown')}\n")
                
                # Extract document section if available
                if 'pbi:mashup' in details and 'document' in details['pbi:mashup']:
                    f.write("\n\nDOCUMENT SECTION:\n")
                    f.write("=" * 50 + "\n")
                    document = details['pbi:mashup']['document']
                    
                    # Parse and format the document section
                    formatted_document = self._format_document_section(document)
                    f.write(formatted_document)
            
            # Also save the document section as a separate file if available
            if 'pbi:mashup' in details and 'document' in details['pbi:mashup']:
                document = details['pbi:mashup']['document']
                doc_filename = f"{safe_workspace_name}_{safe_dataflow_name}_{timestamp}_DOCUMENT.json"
                doc_filepath = os.path.join(self.outputs_dir, doc_filename)
                
                with open(doc_filepath, 'w', encoding='utf-8') as f:
                    f.write(json.dumps(document, indent=2))
                
                print(f"📄 Document saved to: {doc_filename}")
            
            return {
                'success': True,
                'filename': filename,
                'dataflow_name': details.get('name'),
                'dataflow_id': dataflow_id,
                'workspace_name': workspace_name,
                'filepath': filepath,
                'document_file': doc_filename if 'pbi:mashup' in details and 'document' in details['pbi:mashup'] else None
            }
            
        except Exception as e:
            return {"error": f"Failed to write file: {str(e)}"}
    
    def download_all_dataflows(self, workspace_id: str) -> Dict:
        """Download all dataflows from a workspace."""
        # Get all dataflows
        dataflows = self.get_workspace_dataflows(workspace_id)
        
        if 'error' in dataflows:
            return dataflows
        
        workspace_name = self._get_workspace_name(workspace_id)
        results = {
            'success': True,
            'workspace_name': workspace_name,
            'workspace_id': workspace_id,
            'total_dataflows': len(dataflows.get('value', [])),
            'downloaded': [],
            'failed': []
        }
        
        print(f"📊 Found {len(dataflows.get('value', []))} dataflows in workspace '{workspace_name}'")
        
        for dataflow in dataflows.get('value', []):
            dataflow_name = dataflow.get('name', 'Unknown')
            dataflow_id = dataflow.get('objectId')
            
            print(f"📥 Downloading dataflow: {dataflow_name}")
            
            result = self.download_single_dataflow(workspace_id, dataflow_id=dataflow_id)
            
            if result.get('success'):
                results['downloaded'].append(result)
                print(f"   ✅ Downloaded: {result['filename']}")
            else:
                results['failed'].append({
                    'dataflow_name': dataflow_name,
                    'dataflow_id': dataflow_id,
                    'error': result.get('error', 'Unknown error')
                })
                print(f"   ❌ Failed: {result.get('error', 'Unknown error')}")
        
        return results
    
    def _format_document_section(self, document: str) -> str:
        """Format the document section with proper headers and line breaks using advanced cleaning."""
        if not document:
            return "No document content available"
        
        # The document comes as a JSON string with escaped characters
        # We need to unescape it first
        import json
        import re
        
        try:
            # Unescape the JSON string
            unescaped_document = json.loads(f'"{document}"')
        except:
            # If JSON parsing fails, try direct replacement
            unescaped_document = document
        
        # Replace escaped characters
        m_code = unescaped_document.replace('\\r\\n', '\r\n')
        m_code = m_code.replace('\\n', '\n')
        m_code = m_code.replace('\\t', '\t')
        m_code = m_code.replace('\\"', '"')
        
        # Split by shared sections
        sections = re.split(r'(\r\nshared\s+\w+\s*=)', m_code)
        
        cleaned_content = []
        
        # Process the first section (before any shared)
        if sections[0].strip():
            cleaned_content.append(sections[0].strip())
            cleaned_content.append("\n\n")
        
        # Process shared sections
        for i in range(1, len(sections), 2):
            if i+1 < len(sections):
                # Extract the shared name
                shared_match = re.match(r'\r\nshared\s+(\w+)\s*=', sections[i])
                if shared_match:
                    shared_name = shared_match.group(1)
                    
                    # Add header
                    cleaned_content.append("=" * 80)
                    cleaned_content.append(f"\n# SHARED: {shared_name}\n")
                    cleaned_content.append("=" * 80)
                    cleaned_content.append("\n\n")
                    
                    # Add the shared declaration and its content
                    cleaned_content.append(f"shared {shared_name} =")
                    
                    # Clean up the section content
                    section_content = sections[i+1]
                    
                    # Remove trailing semicolon if it's followed by another shared
                    if i+2 < len(sections):
                        section_content = re.sub(r';\s*$', '', section_content)
                    
                    cleaned_content.append(section_content)
                    cleaned_content.append(";\n\n")
        
        # Join all cleaned content
        final_content = ''.join(cleaned_content)
        
        # Additional formatting improvements
        # Fix indentation for let...in blocks
        final_content = self._format_let_in_blocks(final_content)
        
        return final_content
    
    def _format_let_in_blocks(self, content):
        """Improve formatting of let...in blocks"""
        lines = content.split('\n')
        formatted_lines = []
        indent_level = 0
        in_let_block = False
        
        for line in lines:
            stripped = line.strip()
            
            # Adjust indent level
            if stripped.startswith('let'):
                in_let_block = True
                formatted_lines.append(' ' * indent_level + stripped)
                indent_level += 4
            elif stripped == 'in':
                indent_level -= 4
                formatted_lines.append(' ' * indent_level + stripped)
                indent_level += 4
            elif stripped.startswith('in '):
                indent_level -= 4
                formatted_lines.append(' ' * indent_level + stripped)
                in_let_block = False
            elif in_let_block and stripped.endswith(','):
                formatted_lines.append(' ' * indent_level + stripped)
            elif in_let_block and not stripped:
                formatted_lines.append('')
            else:
                # Check if we're ending a let block
                if in_let_block and (stripped.endswith(';') or 'in\n' in line):
                    indent_level = max(0, indent_level - 4)
                    in_let_block = False
                formatted_lines.append(' ' * indent_level + stripped)
        
        return '\n'.join(formatted_lines)
    
    def _get_workspace_name(self, workspace_id: str) -> str:
        """Get workspace name from workspace ID using Power BI API."""
        try:
            url = f"{self.base_url}/groups/{workspace_id}"
            result = self.make_request('GET', url)
            if 'error' not in result:
                return result.get('name', 'Unknown Workspace')
        except:
            pass
        return "Unknown Workspace"


def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Download dataflows from Microsoft Power BI')
    parser.add_argument('workspace_id', nargs='?', help='Workspace ID')
    parser.add_argument('--dataflow-name', help='Specific dataflow name to download')
    parser.add_argument('--dataflow-id', help='Specific dataflow ID to download')
    parser.add_argument('--all', action='store_true', help='Download all dataflows in workspace')
    parser.add_argument('--test', action='store_true', help='Test API connection with a simple request')
    
    args = parser.parse_args()
    
    downloader = DataflowDownloader()
    
    if args.test:
        print("🧪 Testing API connection...")
        # Test with the known working endpoint
        test_workspace_id = "597c6110-9e6e-43fe-88dd-d15ed73f60e1"
        test_dataflow_id = "c5b6dcd2-e9ee-460a-800e-3b4cce7ea064"
        
        result = downloader.get_dataflow_details(test_workspace_id, test_dataflow_id)
        
        if 'error' in result:
            print(f"❌ Test failed: {result['error']}")
        else:
            print("✅ Test successful!")
            print(f"📊 Dataflow name: {result.get('name', 'Unknown')}")
            print(f"📝 Description: {result.get('description', 'No description')}")
            print(f"🔄 Version: {result.get('version', 'Unknown')}")
            print(f"📅 Modified: {result.get('modifiedTime', 'Unknown')}")
            
            # Show queries if available
            if 'pbi:mashup' in result and 'queriesMetadata' in result['pbi:mashup']:
                queries = result['pbi:mashup']['queriesMetadata']
                print(f"📋 Queries found: {len(queries)}")
                for query_name in list(queries.keys())[:5]:  # Show first 5
                    print(f"   • {query_name}")
                if len(queries) > 5:
                    print(f"   ... and {len(queries) - 5} more")
        return
    
    if args.all:
        print("🚀 Downloading all dataflows...")
        result = downloader.download_all_dataflows(args.workspace_id)
        
        if result.get('success'):
            print(f"\n✅ Download Complete!")
            print(f"📊 Workspace: {result['workspace_name']}")
            print(f"📄 Downloaded: {len(result['downloaded'])} dataflows")
            print(f"❌ Failed: {len(result['failed'])} dataflows")
            
            if result['downloaded']:
                print(f"\n📁 Downloaded Files:")
                for item in result['downloaded']:
                    print(f"   • {item['dataflow_name']} -> {item['filename']}")
            
            if result['failed']:
                print(f"\n❌ Failed Downloads:")
                for item in result['failed']:
                    print(f"   • {item['dataflow_name']}: {item['error']}")
        else:
            print(f"❌ Error: {result.get('error', 'Unknown error')}")
    
    else:
        # Download single dataflow
        if not args.dataflow_name and not args.dataflow_id:
            print("❌ Error: Must specify --dataflow-name or --dataflow-id")
            return
        
        print("🚀 Downloading dataflow...")
        result = downloader.download_single_dataflow(
            args.workspace_id,
            dataflow_name=args.dataflow_name,
            dataflow_id=args.dataflow_id
        )
        
        if result.get('success'):
            print(f"✅ Successfully downloaded dataflow!")
            print(f"📄 File: {result['filename']}")
            print(f"📊 Dataflow: {result['dataflow_name']}")
            print(f"🏢 Workspace: {result['workspace_name']}")
        else:
            print(f"❌ Error: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    main() 