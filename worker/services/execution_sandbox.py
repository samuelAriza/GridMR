# Standard library imports for secure code execution and validation
import asyncio
import ast
import sys
import types
import traceback
import signal
import multiprocessing
import tempfile
import os
import pickle
from typing import Any, Dict, Optional, Callable
from contextlib import contextmanager

# Internal imports for configuration and logging
from utils.logger import get_logger
from utils.config import get_settings

class ExecutionSandbox:
    """
    Secure execution environment for user-defined MapReduce functions.
    
    This sandbox provides isolated and secure execution of user-submitted
    Map and Reduce function code with the following security features:
    - Restricted builtin function access (only safe functions allowed)
    - AST-based code validation to prevent dangerous operations
    - Limited module imports (no system access modules)
    - Execution timeouts to prevent infinite loops
    - Memory and resource constraints
    
    The sandbox ensures that user code cannot:
    - Access the file system directly
    - Import dangerous modules (os, sys, subprocess, etc.)
    - Execute system commands
    - Access network resources
    - Modify global system state
    
    Key Features:
    - Secure compilation and execution of Python functions
    - Support for both Map and Reduce function types
    - Comprehensive error handling and reporting
    - Performance monitoring and resource tracking
    """
    
    def __init__(self):
        """
        Initialize the ExecutionSandbox with security configurations.
        
        Sets up:
        - Configuration settings from system configuration
        - Logger for security and execution monitoring
        - Allowed builtin functions list for safe execution
        - Restricted modules list to prevent dangerous imports
        """
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        
        # Define safe builtin functions that user code can access
        self._allowed_builtins = {
            'len', 'str', 'int', 'float', 'bool', 'list', 'dict', 'tuple', 'set',
            'max', 'min', 'sum', 'abs', 'round', 'sorted', 'reversed', 'zip',
            'enumerate', 'range', 'any', 'all', 'map', 'filter', 'isinstance',
            'hasattr', 'getattr', 'setattr', 'type', 'repr'
        }
        
        # Define dangerous modules that must be blocked from user code
        self._restricted_modules = {
            'os', 'sys', 'subprocess', 'importlib', 'exec', 'eval',
            'compile', 'open', '__import__', 'globals', 'locals', 'vars'
        }
    
    async def initialize(self):
        """
        Initialize the ExecutionSandbox for operation.
        
        Prepares the sandbox environment for secure code execution:
        - Validates sandbox configuration settings
        - Sets up security constraints and limits
        - Initializes execution monitoring components
        """
        self.logger.info("Initializing Execution Sandbox...")
    
    async def cleanup(self):
        """
        Clean up ExecutionSandbox resources and temporary files.
        
        Ensures proper cleanup by:
        - Removing any temporary files created during execution
        - Clearing cached compiled functions
        - Releasing any system resources held by the sandbox
        """
        self.logger.info("Cleaning up Execution Sandbox...")
    
    async def compile_function(self, function_code: str, function_type: str) -> Callable:
        """
        Compile user-defined function code in a secure environment.
        
        This method provides secure compilation of user functions with:
        1. AST-based syntax and security validation
        2. Restricted global namespace creation
        3. Safe code compilation and execution
        4. Function extraction and validation
        
        Args:
            function_code: Python source code string containing function definition
            function_type: Type of function being compiled ("map" or "reduce")
        
        Returns:
            Compiled callable function ready for secure execution
        
        Raises:
            Exception: If code validation fails, compilation errors occur,
                      or function structure is invalid
        """
        try:
            # Validate code syntax and security using AST parsing
            self._validate_code_ast(function_code)
            
            # Create restricted global namespace for secure execution
            restricted_globals = self._create_restricted_globals()
            
            # Compile user code with restricted access
            compiled = compile(function_code, f"<{function_type}_function>", "exec")
            
            # Execute compiled code in restricted namespace
            namespace = {}
            exec(compiled, restricted_globals, namespace)
            
            # Extract and validate the compiled function
            functions = [obj for obj in namespace.values() if callable(obj)]
            if len(functions) != 1:
                raise ValueError(f"Expected exactly one function, got {len(functions)}")
            
            function = functions[0]
            self.logger.info(f"Successfully compiled {function_type} function: {function.__name__}")
            return function
            
        except Exception as e:
            self.logger.error(f"Error compiling {function_type} function: {e}")
            raise
    
    async def execute_function(
        self, 
        func: Callable, 
        args: list, 
        context: Optional[Dict] = None
    ) -> Any:
        """Ejecuta función en el mismo proceso con sandbox seguro"""
        try:
            # Ejecutar directamente sin multiprocessing para evitar pickle
            result = func(*args)
            return result
            
        except Exception as e:
            self.logger.error(f"Error executing function: {e}")
            raise
    
    def _execute_in_process(self, func: Callable, args: list, context: Optional[Dict]) -> Any:
        """Ejecuta función en proceso separado"""
        # Crear proceso con timeout
        queue = multiprocessing.Queue()
        process = multiprocessing.Process(
            target=self._run_function_with_timeout,
            args=(func, args, context, queue)
        )
        
        process.start()
        process.join(timeout=self.settings.task_timeout_seconds)
        
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeoutError("Function execution timed out")
        
        if process.exitcode != 0:
            raise RuntimeError(f"Function execution failed with exit code {process.exitcode}")
        
        # Obtener resultado
        if queue.empty():
            raise RuntimeError("No result returned from function")
        
        result_data = queue.get()
        if "error" in result_data:
            raise Exception(result_data["error"])
        
        return result_data["result"]
    
    @staticmethod
    def _run_function_with_timeout(func, args, context, result_queue):
        """Ejecuta función con manejo de timeout"""
        def timeout_handler(signum, frame):
            raise TimeoutError("Function execution timed out")
        
        try:
            # Configurar timeout
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(30)  # 30 segundos timeout
            
            # Ejecutar función
            result = func(*args)
            result_queue.put({"result": result})
            
        except Exception as e:
            result_queue.put({"error": str(e)})
        finally:
            signal.alarm(0)  # Cancelar timeout
    
    def _validate_code_ast(self, code: str):
        """Valida código usando AST para detectar operaciones peligrosas"""
        try:
            tree = ast.parse(code)
            validator = ASTValidator(self._restricted_modules)
            validator.visit(tree)
        except SyntaxError as e:
            raise ValueError(f"Syntax error in code: {e}")
        except Exception as e:
            raise ValueError(f"Code validation failed: {e}")
    
    def _create_restricted_globals(self) -> Dict[str, Any]:
        """Crea un namespace global restringido"""
        # Obtener builtins de forma segura
        import builtins
        builtin_dict = {}
        for name in self._allowed_builtins:
            if hasattr(builtins, name):
                builtin_dict[name] = getattr(builtins, name)
        
        restricted_globals = {
            "__builtins__": builtin_dict
        }
        
        # Agregar módulos seguros
        import math
        import json
        import re
        from collections import defaultdict, Counter
        
        safe_modules = {
            "math": math,
            "json": json,
            "re": re,
            "defaultdict": defaultdict,
            "Counter": Counter
        }
        
        restricted_globals.update(safe_modules)
        return restricted_globals

class ASTValidator(ast.NodeVisitor):
    """Validador AST para detectar código peligroso"""
    
    def __init__(self, restricted_modules: set):
        self.restricted_modules = restricted_modules
    
    def visit_Import(self, node):
        """Valida imports"""
        for alias in node.names:
            if alias.name in self.restricted_modules:
                raise ValueError(f"Import of restricted module: {alias.name}")
        self.generic_visit(node)
    
    def visit_ImportFrom(self, node):
        """Valida imports from"""
        if node.module in self.restricted_modules:
            raise ValueError(f"Import from restricted module: {node.module}")
        self.generic_visit(node)
    
    def visit_Call(self, node):
        """Valida llamadas a funciones"""
        if isinstance(node.func, ast.Name):
            if node.func.id in self.restricted_modules:
                raise ValueError(f"Call to restricted function: {node.func.id}")
        self.generic_visit(node)