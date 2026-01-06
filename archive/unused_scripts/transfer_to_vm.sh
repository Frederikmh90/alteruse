#!/bin/bash

# File Transfer Script for VM Setup
# Usage: ./transfer_to_vm.sh user@vm-ip-address

if [ $# -eq 0 ]; then
    echo "Usage: $0 user@vm-ip-address"
    echo "Example: $0 ubuntu@192.168.1.100"
    exit 1
fi

VM_ADDRESS="$1"
PROJECT_NAME="alteruse"

echo "🚀 Transferring files to VM: $VM_ADDRESS"
echo "This will transfer approximately 1.6GB of data"
echo ""

# Create remote directory structure
echo "📁 Creating remote directory structure..."
ssh "$VM_ADDRESS" "mkdir -p ~/$PROJECT_NAME/data/Samlet_06112025"
ssh "$VM_ADDRESS" "mkdir -p ~/$PROJECT_NAME/notebooks/url_extraction"

# Transfer pipeline scripts
echo "📄 Transferring pipeline scripts..."
scp -r notebooks/url_extraction/*.py "$VM_ADDRESS:~/alteruse/notebooks/url_extraction/"

# Transfer browser data (this is the large part)
echo "📊 Transferring browser data (1.6GB)..."
echo "This may take several minutes depending on your connection speed..."
scp -r data/Samlet_06112025/Browser/ "$VM_ADDRESS:~/alteruse/data/Samlet_06112025/"

# Transfer configuration files
echo "⚙️ Transferring configuration files..."
scp vm_requirements.txt "$VM_ADDRESS:~/alteruse/"
scp vm_setup_guide.md "$VM_ADDRESS:~/alteruse/"
scp run_pipeline.sh "$VM_ADDRESS:~/alteruse/"

# Make pipeline script executable on VM
ssh "$VM_ADDRESS" "chmod +x ~/$PROJECT_NAME/run_pipeline.sh"

echo ""
echo "✅ Transfer completed!"
echo ""
echo "📋 Next steps on the VM:"
echo "1. SSH into the VM: ssh $VM_ADDRESS"
echo "2. Navigate to project: cd ~/$PROJECT_NAME"
echo "3. Create virtual environment: python3 -m venv venv"
echo "4. Activate environment: source venv/bin/activate"
echo "5. Install dependencies: pip install -r vm_requirements.txt"
echo "6. Run the pipeline: ./run_pipeline.sh"
echo ""
echo "📖 For detailed instructions, see: vm_setup_guide.md" 